/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class has the control logic for the Leader.
 * Leader一定是单例的
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);

    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal  extends SyncedLearnerTracker {
        public QuorumPacket packet;
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

    // Throttle when there are too many concurrent snapshots being sent to observers
    private static final String MAX_CONCURRENT_SNAPSHOTS = "zookeeper.leader.maxConcurrentSnapshots";
    private static final int maxConcurrentSnapshots;
    private static final String MAX_CONCURRENT_SNAPSHOT_TIMEOUT = "zookeeper.leader.maxConcurrentSnapshotTimeout";
    private static final long maxConcurrentSnapshotTimeout;
    static {
        maxConcurrentSnapshots = Integer.getInteger(MAX_CONCURRENT_SNAPSHOTS, 10);
        LOG.info(MAX_CONCURRENT_SNAPSHOTS + " = " + maxConcurrentSnapshots);
        maxConcurrentSnapshotTimeout = Long.getLong(MAX_CONCURRENT_SNAPSHOT_TIMEOUT, 5);
        LOG.info(MAX_CONCURRENT_SNAPSHOT_TIMEOUT + " = " + maxConcurrentSnapshotTimeout);
    }

    private final LearnerSnapshotThrottler learnerSnapshotThrottler;

    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

    // VisibleForTesting
    protected boolean quorumFormed = false;

    // the follower acceptor thread
    volatile LearnerCnxAcceptor cnxAcceptor = null;

    // list of all the followers
    private final HashSet<LearnerHandler> learners =
        new HashSet<LearnerHandler>();

    private final BufferStats proposalStats;

    public BufferStats getProposalStats() {
        return proposalStats;
    }

    public LearnerSnapshotThrottler createLearnerSnapshotThrottler(
            int maxConcurrentSnapshots, long maxConcurrentSnapshotTimeout) {
        return new LearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers =
        new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners =
        new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    //存的都是在等待某个事务提交完成，发出sync响应的请求。
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs =
        new HashMap<Long,List<LearnerSyncRequest>>();

    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * Adds peer to the leader.
     *
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     *
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);
        }
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }
    }


    /**
     * Returns true if a quorum in qv is connected and synced with the leader
     * and false otherwise
     *  
     * @param qv, a QuorumVerifier
     */
    public boolean isQuorumSynced(QuorumVerifier qv) {
       HashSet<Long> ids = new HashSet<Long>();
       if (qv.getVotingMembers().containsKey(self.getId()))
           ids.add(self.getId());
       synchronized (forwardingFollowers) {
           for (LearnerHandler learnerHandler: forwardingFollowers){
               if (learnerHandler.synced() && qv.getVotingMembers().containsKey(learnerHandler.getSid())){
                   ids.add(learnerHandler.getSid());
               }
           }
       }
       return qv.containsQuorum(ids);
    }
    
    private final ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new BufferStats();
        try {
            if (self.shouldUsePortUnification() || self.isSslQuorum()) {
                boolean allowInsecureConnection = self.shouldUsePortUnification();
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection, self.getQuorumAddress().getPort());
                } else {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection);
                }
            } else {
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new ServerSocket(self.getQuorumAddress().getPort());
                } else {
                    ss = new ServerSocket();
                }
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk = zk;
        this.learnerSnapshotThrottler = createLearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * This message is for follower to expect diff
     * 此消息供follower预期差异，一般用空的表示无差异
     */
    final static int DIFF = 13;

    /**
     * This is for follower to truncate its logs
     * 这是为了让follower删除它的事务日志
     */
    final static int TRUNC = 14;

    /**
     * This is for follower to download the snapshots
     * 这是供follower下载快照的
     */
    final static int SNAP = 15;

    /**
     * This tells the leader that the connecting peer is actually an observer
     * 这告诉领导者，连接的服务实例实际上是观察者
     */
    final static int OBSERVERINFO = 16;

    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     * follower发送此消息类型以传递最后的zxid。 这是为了向后兼容。
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     * leader发给follower，表示follower数据已经同步更新完成，可以响应客户端请求了。
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;

    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     *
     * 此消息类型发送给leader以请求和进行变异操作。 有效负载将由一个请求标头和一个请求组成。
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     * 领导者发送此消息类型以提出提案。
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     * leader发给follower，让follower提交提案。
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     * 由follower向leader发起ping请求，用来确定leader存活。
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;

    /**
     * This message type informs observers of a committed proposal.
     * 通知Observer，发送的数据是刚刚表决通过并已提交的提案。
     */
    final static int INFORM = 8;
    
    /**
     * Similar to COMMIT, only for a reconfig operation.
     */
    final static int COMMITANDACTIVATE = 9;
    
    /**
     * Similar to INFORM, only for a reconfig operation.
     */
    final static int INFORMANDACTIVATE = 19;

    //提案，包括从将要发送出去的 到 还没完成投票commit的提案。 key：zxid，value：提案
    final ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    //通过投票，将被应用的提案
    private final ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();

    // VisibleForTesting
    protected final Proposal newLeaderProposal = new Proposal();

    class LearnerCnxAcceptor extends ZooKeeperCriticalThread {
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress(), zk
                    .getZooKeeperServerListener());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    Socket s = null;
                    boolean error = false;
                    try {
                        //监听集群通讯端口
                        s = ss.accept();

                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        //为每个learner创建一个通信线程，专门负责与这个follower的所有通信
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        error = true;
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e){
                        LOG.error("Exception while connecting to quorum learner", e);
                        error = true;
                    } catch (Exception e) {
                        error = true;
                        throw e;
                    } finally {
                        // Don't leak sockets on errors
                        if (error && s != null && !s.isClosed()) {
                            try {
                                s.close();
                            } catch (IOException e) {
                                LOG.warn("Error closing socket", e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e.getMessage());
                handleException(this.getName(), e);
            }
        }

        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary;

    long epoch = -1;
    boolean waitingForNewEpoch = true;

    // when a reconfig occurs where the leader is removed or becomes an observer, 
   // it does not commit ops after committing the reconfig
    boolean allowedToCommit = true;     
    /**
     * This method is main function that is called to lead
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        //fast leader election选主耗费的时长 ms
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        //在follower确定自己是follower后，follower首先会主动连接leader，发送自己的数据信息
        //而leader在确定自己是leader后，不会主动去连接follower，而是等着follower来连。
        //leader收到follower的信息后，创建并启动learnerHandler线程，交给该线程处理，内部会调用投票算法，算出最新的epoch，
        //然后leader会将确定的最新epoch等发给所有follower。
        //总结就是选举过后，确定了自己的角色，然后follower发的第一个包是自己的信息，leader发的第一个包是确定后的新纪元等信息。

        try {
            self.tick.set(0);
            zk.loadData();

            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // Start thread that waits for connection requests from
            // new followers.
            //learner与leader连接ServerSocket.accept线程
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();

            //选举后，每个follower确定自己是follower后，会主动连接leader，发送sid和zxid等，
            //leader起的LearnerHandler线程在接收到follower消息后，也会调这个方法，相当于参与投票，帮助leader确定新的epoch
            //投票最终结果没有出来之前，此处一直阻塞等待
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());

            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));

            synchronized(this){
                lastProposed = zk.getZxid();
            }

            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                   null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            QuorumVerifier curQV = self.getQuorumVerifier();
            if (curQV.getVersion() == 0 && curQV.getVersion() == lastSeenQV.getVersion()) {
                // This was added in ZOOKEEPER-1783. The initial config has version 0 (not explicitly
                // specified by the user; the lack of version in a config file is interpreted as version=0). 
                // As soon as a config is established we would like to increase its version so that it
                // takes presedence over other initial configs that were not established (such as a config
                // of a server trying to join the ensemble, which may be a partial view of the system, not the full config). 
                // We chose to set the new version to the one of the NEWLEADER message. However, before we can do that
                // there must be agreement on the new version, so we can only change the version when sending/receiving UPTODATE,
                // not when sending/receiving NEWLEADER. In other words, we can't change curQV here since its the committed quorum verifier, 
                // and there's still no agreement on the new version that we'd like to use. Instead, we use 
                // lastSeenQuorumVerifier which is being sent with NEWLEADER message
                // so its a good way to let followers know about the new version. (The original reason for sending 
                // lastSeenQuorumVerifier with NEWLEADER is so that the leader completes any potentially uncommitted reconfigs
                // that it finds before starting to propose operations. Here we're reusing the same code path for 
                // reaching consensus on the new version number.)
                
                // It is important that this is done before the leader executes waitForEpochAck,
                // so before LearnerHandlers return from their waitForEpochAck
                // hence before they construct the NEWLEADER message containing
                // the last-seen-quorumverifier of the leader, which we change below
               try {
                   QuorumVerifier newQV = self.configFromString(curQV.toString());
                   newQV.setVersion(zk.getZxid());
                   self.setLastSeenQuorumVerifier(newQV, true);    
               } catch (Exception e) {
                   throw new IOException(e);
               }
            }
            
            newLeaderProposal.addQuorumVerifier(self.getQuorumVerifier());
            if (self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()){
               newLeaderProposal.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }
            
            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            //看learnerHandler的逻辑，leader在确认完新epoch后，会将该epoch发给所有follower
            //此处是等待拿到follower对新纪元的确认响应，然后投票，确定多数都统一了，阻塞等待结束
             waitForEpochAck(self.getId(), leaderStateSummary);
             //集群纪元统一了
             self.setCurrentEpoch(epoch);    
            
             try {
                 //阻塞等待NEWLADER包的响应，
                 waitForNewLeaderAck(self.getId(), zk.getZxid());
             } catch (InterruptedException e) {
                 shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                         + newLeaderProposal.ackSetsToString() + " ]");
                 HashSet<Long> followerSet = new HashSet<Long>();

                 for(LearnerHandler f : getLearners()) {
                     if (self.getQuorumVerifier().getVotingMembers().containsKey(f.getSid())){
                         followerSet.add(f.getSid());
                     }
                 }    
                 boolean initTicksShouldBeIncreased = true;
                 for (Proposal.QuorumVerifierAcksetPair qvAckset:newLeaderProposal.qvAcksetPairs) {
                     if (!qvAckset.getQuorumVerifier().containsQuorum(followerSet)) {
                         initTicksShouldBeIncreased = false;
                         break;
                     }
                 }                  
                 if (initTicksShouldBeIncreased) {
                     LOG.warn("Enough followers present. "+
                             "Perhaps the initTicks need to be increased.");
                 }
                 return;
             }

             startZkServer();
             
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             *
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }

            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.setZooKeeperServer(zk);
            }

            self.adminServer.setZooKeeperServer(zk);

            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;
            // If not null then shutdown this leader
            String shutdownMessage = null;

            while (true) {
                //leader不断循环，主要是做两件事：
                // 1.收集LearnerHandler线程还存活 并且 与leader失联时长未超过超时时间的sid，
                //    判断是否还有超半数的具有投票权的服务器存活，确保集群仍能具备投票的基本条件。如果不具备，跳出循环，重新选主。
                // 2.ping所有learner，更新上面说的超时时间。
                synchronized (this) {
                    long start = Time.currentElapsedTime();
                    long cur = start;
                    long end = start + self.tickTime / 2;
                    while (cur < end) {
                        wait(end - cur);
                        cur = Time.currentElapsedTime();
                    }

                    if (!tickSkip) {
                        self.tick.incrementAndGet();
                    }

                    // We use an instance of SyncedLearnerTracker to
                    // track synced learners to make sure we still have a
                    // quorum of current (and potentially next pending) view.
                    //我们使用SyncedLearnerTracker实例来跟踪已同步的学习者，以确保我们仍具有当前（以及潜在的下一个未决）视图的法定人数。
                    SyncedLearnerTracker syncedAckSet = new SyncedLearnerTracker();
                    syncedAckSet.addQuorumVerifier(self.getQuorumVerifier());
                    if (self.getLastSeenQuorumVerifier() != null
                            && self.getLastSeenQuorumVerifier().getVersion() > self
                                    .getQuorumVerifier().getVersion()) {
                        syncedAckSet.addQuorumVerifier(self
                                .getLastSeenQuorumVerifier());
                    }

                    syncedAckSet.addAck(self.getId());

                    for (LearnerHandler f : getLearners()) {
                        if (f.synced()) {
                            //learnerHandler线程存活，并且收到learner通信的时间未超过存活超时时间
                            //这里就是收集所有还能正常运行和通信的learner的sid
                            syncedAckSet.addAck(f.getSid());
                        }
                    }

                    // check leader running status
                    if (!this.isRunning()) {
                        // set shutdown flag
                        shutdownMessage = "Unexpected internal error";
                        break;
                    }

                    if (!tickSkip && !syncedAckSet.hasAllQuorums()) {
                        // Lost quorum of last committed and/or last proposed
                        // config, set shutdown flag
                        shutdownMessage = "Not sufficient followers synced, only synced with sids: [ "
                                + syncedAckSet.ackSetsToString() + " ]";
                        break;
                    }
                    tickSkip = !tickSkip;
                }

                //leader主动发ping给所有learner
                for (LearnerHandler f : getLearners()) {
                    f.ping();
                }
            }
            if (shutdownMessage != null) {
                shutdown(shutdownMessage);
                // leader goes in looking state
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }

        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }

        // NIO should not accept conenctions
        self.setZooKeeperServer(null);
        self.adminServer.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        self.closeAllConnections();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /** In a reconfig operation, this method attempts to find the best leader for next configuration.
     *  If the current leader is a voter in the next configuartion, then it remains the leader.
     *  Otherwise, choose one of the new voters that acked the reconfiguartion, such that it is as   
     * up-to-date as possible, i.e., acked as many outstanding proposals as possible.
     *  
     * @param reconfigProposal
     * @param zxid of the reconfigProposal
     * @return server if of the designated leader
     */
    
    private long getDesignatedLeader(Proposal reconfigProposal, long zxid) {
       //new configuration
       Proposal.QuorumVerifierAcksetPair newQVAcksetPair = reconfigProposal.qvAcksetPairs.get(reconfigProposal.qvAcksetPairs.size()-1);        
       
       //check if I'm in the new configuration with the same quorum address - 
       // if so, I'll remain the leader    
       if (newQVAcksetPair.getQuorumVerifier().getVotingMembers().containsKey(self.getId()) && 
               newQVAcksetPair.getQuorumVerifier().getVotingMembers().get(self.getId()).addr.equals(self.getQuorumAddress())){  
           return self.getId();
       }
       // start with an initial set of candidates that are voters from new config that 
       // acknowledged the reconfig op (there must be a quorum). Choose one of them as 
       // current leader candidate
       HashSet<Long> candidates = new HashSet<Long>(newQVAcksetPair.getAckset());
       candidates.remove(self.getId()); // if we're here, I shouldn't be the leader
       long curCandidate = candidates.iterator().next();
       
       //go over outstanding ops in order, and try to find a candidate that acked the most ops.
       //this way it will be the most up-to-date and we'll minimize the number of ops that get dropped
       
       long curZxid = zxid + 1;
       Proposal p = outstandingProposals.get(curZxid);
               
       while (p!=null && !candidates.isEmpty()) {                              
           for (Proposal.QuorumVerifierAcksetPair qvAckset: p.qvAcksetPairs){ 
               //reduce the set of candidates to those that acknowledged p
               candidates.retainAll(qvAckset.getAckset());
               //no candidate acked p, return the best candidate found so far
               if (candidates.isEmpty()) return curCandidate;
               //update the current candidate, and if it is the only one remaining, return it
               curCandidate = candidates.iterator().next();
               if (candidates.size() == 1) return curCandidate;
           }      
           curZxid++;
           p = outstandingProposals.get(curZxid);
       }
       
       return curCandidate;
    }

    /**
     * 尝试commit事务，true=成功，false=失败。
     * @return True if committed, otherwise false.
     **/
    synchronized public boolean tryToCommit(Proposal p, long zxid, SocketAddress followerAddr) {       
       // make sure that ops are committed in order. With reconfigurations it is now possible
       // that different operations wait for different sets of acks, and we still want to enforce
       // that they are committed in order. Currently we only permit one outstanding reconfiguration
       // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
       // pending all wait for a quorum of old and new config, so it's not possible to get enough acks
       // for an operation without getting enough acks for preceding ops. But in the future if multiple
       // concurrent reconfigs are allowed, this can happen.
       // 确保操作按顺序提交。 通过重新配置，现在有可能不同的操作等待不同的ack组，而我们仍然要强制要求它们按顺序提交。
        // 当前，我们仅允许进行一次未完成的重新配置，以便在重新配置未决期间提出的重新配置和随后提出的未完成操作都将等待一定数量的新旧配置，
        // 因此，如果没有为先前的操作获得足够的确认，就不可能获得足够的操作确认。
        // 但是将来如果允许多个并发重新配置，则可能会发生这种情况。

       //用入参的zxid-1，作为新的zxid去outstandingProposals中取对应的提案，
       // 如果不为空的话，就表示前一个提案还没有提交，不符合提交顺序，zk强制要求它们按顺序提交。
       if (outstandingProposals.containsKey(zxid - 1)) return false;
       
       // in order to be committed, a proposal must be accepted by a quorum.
       //
       // getting a quorum from all necessary configurations.
       // 投票未达半数，投票不通过
        if (!p.hasAllQuorums()) {
           return false;                 
        }
        
        // commit proposals in order
        //当前待提交的zxid不是紧跟在上一次提交的zxid之后的，不是按顺序提交。
        if (zxid != lastCommitted+1) {    
           LOG.warn("Commiting zxid 0x" + Long.toHexString(zxid)
                    + " from " + followerAddr + " not first!");
            LOG.warn("First is "
                    + (lastCommitted+1));
        }     

        //移除提案
        outstandingProposals.remove(zxid);
        
        if (p.request != null) {
             //将该提案请求添加到待应用到内存DBTree的队列中
             toBeApplied.add(p);
        }

        if (p.request == null) {
            LOG.warn("Going to commmit null: " + p);
        } else if (p.request.getHdr().getType() == OpCode.reconfig) {                                   
            LOG.debug("Committing a reconfiguration! " + outstandingProposals.size()); 
                 
            //if this server is voter in new config with the same quorum address, 
            //then it will remain the leader
            //otherwise an up-to-date follower will be designated as leader. This saves
            //leader election time, unless the designated leader fails                             
            Long designatedLeader = getDesignatedLeader(p, zxid);
            //LOG.warn("designated leader is: " + designatedLeader);

            QuorumVerifier newQV = p.qvAcksetPairs.get(p.qvAcksetPairs.size()-1).getQuorumVerifier();
       
            self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);

            if (designatedLeader != self.getId()) {
                allowedToCommit = false;
            }
                   
            // we're sending the designated leader, and if the leader is changing the followers are 
            // responsible for closing the connection - this way we are sure that at least a majority of them 
            // receive the commit message.
            commitAndActivate(zxid, designatedLeader);
            informAndActivate(p, designatedLeader);
            //turnOffFollowers();
        } else {
            //创建发送commit包给follower，通知他们提交请求。
            commit(zxid);
            //创建发送INFORM包给observer，通知他们接收请求中包含的事务数据。
            inform(p);
        }

        //调用CommitProcessor.commit，提交请求。
        zk.commitProcessor.commit(p.request);
        //提交完请求后，可以发出等待该事务提交完成，数据同步的sync响应了
        if(pendingSyncs.containsKey(zxid)){
            for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                sendSync(r);
            }               
        } 
        
        return  true;   
    }
    
    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     *
     * 保留leader收到的提案应答的计数
     *
     * @param zxid, the zxid of the proposal sent out
     * @param sid, the id of the server that sent the ack
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (!allowedToCommit) return; // last op committed was a leader change - from now on 
                                     // the new leader should commit        
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }
        
        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack with this method. However,
             * the learner sends an ack back to the leader after it gets
             * UPTODATE, so we just ignore the message.
             */
            return;
        }
            
            
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }

        //根据zxid取出提案
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }

        //只要是收到了投票人的应答，就表示投票人赞同提案。
        //将投票人的sid放到所有赞同的投票人set中。
        p.addAck(sid);        
        /*if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }*/
        
        boolean hasCommitted = tryToCommit(p, zxid, followerAddr);

        // If p is a reconfiguration, multiple other operations may be ready to be committed,
        // since operations wait for different sets of acks.
       // Currently we only permit one outstanding reconfiguration at a time
       // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
       // pending all wait for a quorum of old and new config, so its not possible to get enough acks
       // for an operation without getting enough acks for preceding ops. But in the future if multiple
       // concurrent reconfigs are allowed, this can happen and then we need to check whether some pending
        // ops may already have enough acks and can be committed, which is what this code does.

        if (hasCommitted && p.request!=null && p.request.getHdr().getType() == OpCode.reconfig){
               long curZxid = zxid;
           while (allowedToCommit && hasCommitted && p!=null){
               curZxid++;
               p = outstandingProposals.get(curZxid);
               if (p !=null) hasCommitted = tryToCommit(p, curZxid, null);             
           }
        }
    }
    
    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        //对leader来说，next为FinalRequestProcessor
        private final RequestProcessor next;

        private final Leader leader;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         *
         * 该请求处理器仅维护toBeApplied列表。将已确定提交，待应用到内存树的请求转发给下一个处理器执行。
         *
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next, Leader leader) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.leader = leader;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            next.processRequest(request);

            // The only requests that should be on toBeApplied are write
            // requests, for which we will have a hdr. We can't simply use
            // request.zxid here because that is set on read requests to equal
            // the zxid of the last write op.
            if (request.getHdr() != null) {
                long zxid = request.getHdr().getZxid();
                //通过投票，将被应用的提案
                Iterator<Proposal> iter = leader.toBeApplied.iterator();
                if (iter.hasNext()) {
                    Proposal p = iter.next();
                    if (p.request != null && p.request.zxid == zxid) {
                        iter.remove();
                        return;
                    }
                }
                LOG.error("Committed request not found on toBeApplied: "
                          + request);
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     *
     * @param qp
     *                the packet to be sent
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            //遍历发给所有的follower
            for (LearnerHandler f : forwardingFollowers) {
                f.queuePacket(qp);
            }
        }
    }

    /**
     * send a packet to all observers
     */
    void sendObserverPacket(QuorumPacket qp) {
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    //使用该值记录最后一次提交的zxid
    //配合当前leader对象锁，和zxid+1 -1 来判断事务是否是按大小顺序提交的
    // 强制按zxid大小顺序提交就是这么做的
    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 创建一个commit包，发到所有具有投票权的follower
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized(this){
            //更新lastCommitted zxid
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }

    //commit and send some info
    public void commitAndActivate(long zxid, long designatedLeader) {
        synchronized(this){
            lastCommitted = zxid;
        }
        
        byte data[] = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(data);                            
       buffer.putLong(designatedLeader);
       
        QuorumPacket qp = new QuorumPacket(Leader.COMMITANDACTIVATE, zxid, data, null);
        sendPacket(qp);
    }

    /**
     * Create an inform packet and send it to all observers.
     * 创建一个通知数据包，并将其发送给所有观察者。
     */
    public void inform(Proposal proposal) {
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid,
                                            proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    
    /**
     * Create an inform&activate packet and send it to all observers.
     */
    public void informAndActivate(Proposal proposal, long designatedLeader) {
       byte[] proposalData = proposal.packet.getData();
        byte[] data = new byte[proposalData.length + 8];
        ByteBuffer buffer = ByteBuffer.wrap(data);                            
       buffer.putLong(designatedLeader);
       buffer.put(proposalData);
       
        QuorumPacket qp = new QuorumPacket(Leader.INFORMANDACTIVATE, proposal.request.zxid, data, null);
        sendObserverPacket(qp);
    }

    //最后表决的提案zxid
    long lastProposed;


    /**
     * Returns the current epoch of the leader.
     *
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }

    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     *
     * 创建一个提案，发送给所有成员
     *
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }

        //序列化头和内容到byte[]
        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastBufferSize(data.length);
        //发的提案、zxid，data
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);

        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;                
        
        synchronized(this) {
           //投票仲裁裁判
           p.addQuorumVerifier(self.getQuorumVerifier());
                   
           if (request.getHdr().getType() == OpCode.reconfig){
               self.setLastSeenQuorumVerifier(request.qv, true);                       
           }
           
           if (self.getQuorumVerifier().getVersion()<self.getLastSeenQuorumVerifier().getVersion()) {
               p.addQuorumVerifier(self.getLastSeenQuorumVerifier());
           }
                   
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            //更新最后投票的zxid
            lastProposed = p.packet.getZxid();
            //暂存的是Proposal
            outstandingProposals.put(lastProposed, p);
            //这里发出的是QuorumPacket
            //遍历发给所有的follower
            sendPacket(pp);
        }
        return p;
    }
    
    public LearnerSnapshotThrottler getLearnerSnapshotThrottler() {
        return learnerSnapshotThrottler;
    }

    /**
     * Process sync requests
     *
     * @param r the request
     */

    synchronized public void processSync(LearnerSyncRequest r){
        if(outstandingProposals.isEmpty()){
            //没有待处理的提议，其中的一层含义是所有提案已经由有效follower表决过，并提交，
            //learder和Follower此时数据已是同步的，直接返回响应即可。
            //入队、序列化、发送响应包。
            sendSync(r);
        } else {
            //获取lastProposed(是个zxid)，对应的所有同步请求
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            //lastProposed是最新的或者说是当前正在处理的提案zxid，同步请求在这个之后到，只要这个提案处理完成就可以发出sync响应了。
            //这里一个隐含的意思是，zxid提交都是按照从小到大顺序提交的，这个最新的提交了，那它之前的所有也就提交了，此时可以发sync响应了。
            //将sync请求对象加到list末尾
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }

    /**
     * Sends a sync message to the appropriate server
     * 发送同步消息到适当的服务器
     */
    public void sendSync(LearnerSyncRequest r){
        //leader发给follower的响应
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }

    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     *
     * @param handler handler of the follower
     * @return last proposed zxid
     * @throws InterruptedException 
     */
    synchronized public long startForwarding(LearnerHandler handler,
            long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        if (lastProposed > lastSeenZxid) {
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }

        return lastProposed;
    }
    // VisibleForTesting
    protected final Set<Long> connectingFollowers = new HashSet<Long>();

    /**
     * 这里是epoch的确定逻辑
     * 在集群启动时第一次连接leader的时候，每个learner会发送自己的server id和最新的zxid。
     * leader收到一个learner的zxid就提取一次epoch，调一次投票仲裁，如果learner的纪元>=当前leader的纪元，
     * 那么将learner的纪元+1作为新纪元。
     * 直到返回纪元的follower数量>半数，就停止投票，使用这个纪元作为新集群的纪元。半数以外的其他follower返回数据就不管了。
     *
     * @param sid learner服务器id
     * @param lastAcceptedEpoch learner最新的纪元epoch
     * @return 确定的新纪元epoch
     */
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        //connectingFollowers 连接的followers
        synchronized(connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                //某一个follower的epoch >= 当前的epoch
                //epoch+1
                epoch = lastAcceptedEpoch+1;
            }
            if (isParticipant(sid)) {
                //是有投票权的follower
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) &&
                                            verifier.containsQuorum(connectingFollowers)) {
                //投票达到半数，投票结束
                waitingForNewEpoch = false;
                //使用这个epoch作为新的epoch
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {
                //未达到半数
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(waitingForNewEpoch && cur < end) {
                    //阻塞直到投票超时结束
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");
                }
            }
            //返回投票最终决定使用的epoch
            return epoch;
        }
    }

    //已投票、表示赞同的follower
    // VisibleForTesting
    protected final Set<Long> electingFollowers = new HashSet<Long>();
    // VisibleForTesting
    protected boolean electionFinished = false;

    //id为follower server id, ss为纪元响应
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        //对epoch纪元进行投票
        synchronized(electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                if (isParticipant(id)) {
                    //服务器id在仲裁裁判的可投票成员里
                    electingFollowers.add(id);
                }
            }
            //投票仲裁裁判
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                //大于半数 选举结束，纪元被统一
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {
                //未达到半数
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(!electionFinished && cur < end) {
                    //阻塞等待投票超时结束
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }
    
    /**
     * Return a list of sid in set as string  
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     */
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + newLeaderProposal.ackSetsToString()
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        
        /*
         * ZOOKEEPER-1324. the leader sends the new config it must complete
         *  to others inside a NEWLEADER message (see LearnerHandler where
         *  the NEWLEADER message is constructed), and once it has enough
         *  acks we must execute the following code so that it applies the
         *  config to itself.
         */
        QuorumVerifier newQV = self.getLastSeenQuorumVerifier();
        
        Long designatedLeader = getDesignatedLeader(newLeaderProposal, zk.getZxid());                                         

        self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);
        if (designatedLeader != self.getId()) {
            allowedToCommit = false;
        }
        
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     *
     * @param sid
     * @throws InterruptedException
     */
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.qvAcksetPairs) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            /*
             * Note that addAck already checks that the learner
             * is a PARTICIPANT.
             */
            newLeaderProposal.addAck(sid);

            if (newLeaderProposal.hasAllQuorums()) {
                quorumFormed = true;
                newLeaderProposal.qvAcksetPairs.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.qvAcksetPairs.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case COMMITANDACTIVATE:
            return "COMMITANDACTIVATE";           
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        case INFORMANDACTIVATE:
            return "INFORMANDACTIVATE";
        default:
            return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getQuorumVerifier().getVotingMembers().containsKey(sid);
    }
}
