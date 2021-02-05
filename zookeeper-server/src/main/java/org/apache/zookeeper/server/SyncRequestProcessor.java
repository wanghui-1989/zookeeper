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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 *
 * 此RequestProcessor将请求记录到磁盘。 它分批处理请求以有效地执行io。
 * 在将请求的日志同步到磁盘之前，该请求不会传递到下一个RequestProcessor。
 * SyncRequestProcessor在3种不同情况下使用：
 * 1.Leader - 将请求同步到磁盘，并将其转发到AckRequestProcessor，后者将ack发回给自己。
 * 2.Follower - 将请求同步到磁盘，并将请求转发到SendAckRequestProcessor，后者将数据包发送到领导者。
 *     SendAckRequestProcessor是可刷新的，这使我们能够将推送数据包强制发送到领导者。
 * 3.Observer - 将提交的请求同步到磁盘（作为INFORM数据包接收）。
 *     它永远不会将确认发送回给领导者，因此nextProcessor将为null。
 *     因为它只包含提交的txns，所以这改变了观察者上txnlog的语义。
 *
 * 可以叫做 同步请求处理器。同步请求日志到磁盘，同步请求到其他服务器，包括投票。
 *
 * 只会创建一个，可以当做单线程来理解。
 *
 * 该处理器的执行时机和主要作用：
 *  1. 对leader来说：
 *      1.1 对于一个写操作，先经过预处理器，封装好要写的数据，然后提交给下一个处理器提案处理器
 *      1.2 提案处理器将要写的数据封装到提案请求中，发给所有follower
 *      1.3 然后执行当前同步处理器，将这个写请求写到事务日志磁盘缓冲区，缓冲区对象LinkedList toFlush，大小一般为1000，超过该值会执行flush磁盘操作。
 *  2. 对follower来说：
 *      2.1 接到leader发来的提案请求后，follower基本就是先执行该处理器，即将这个写请求写到事务日志磁盘缓冲区。
 *  3. 对leader和follower来说，在写完一定数量的写请求后，会随机触发拍快照操作。
 *
 *  只要能写完事务缓存，没有异常中断，就表示赞同提案，调下一个处理器发送应答给leader。
 *  相当于只要发了应答，就表示赞同提案，出现异常或者错误、网络问题等导致没有发应答给leader，就表示不赞同。
 *  没有中间态度，而且只要能将事务写到磁盘，就一定要投赞同票。
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    //ZooKeeperServer有多个子类
    //QuorumZooKeeperServer：LeaderZooKeeperServer、FollowerZooKeeperServer、LearnerZooKeeperServer、ObserverZooKeeperServer
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    /**
     * 1. 对leader来说，next为AckRequestProcessor
     * 2. 对follower来说，next为SendAckRequestProcessor
     */
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     *
     * 已写入并等待刷新到磁盘的事务。
     * 基本上，这是同步项目列表，在刷新成功返回后，将调用这些回调的回调。
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    /**
     * The number of log entries to log before starting a snapshot
     * 开始快照之前要记录的日志条目数
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            //我们这样做是为了确保并非集合中的所有服务器都同时拍摄快照
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    //初始状态或者刚刚执行完flush磁盘，此时toFlush为空，阻塞取
                    si = queuedRequests.take();
                } else {
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                if (si == requestOfDeath) {
                    //毒丸
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    //将请求封装为事务格式写入事务日志的磁盘缓冲区，但是不flush，返回写入是否成功
                    //zks的多个子类，当前对象是leader、follower、observer的一种
                    if (zks.getZKDatabase().append(si)) {
                        //写日志计数+1
                        logCount++;

                        //算下这个值 上面定义 randRoll = r.nextInt(snapCount/2);
                        //假设snapCount=50，randRoll=r.nextInt(25)，即0-24，假如randRoll=13
                        //logCount > (50/2 + 13) 即 logCount > 38，此时会拍摄快照。
                        //再算个极值 randRoll=snapCount/2  那么logCount > (snapCount/2 + snapCount/2)
                        //可以这么理解 如果没有这个随机数逻辑的话，那就是在 logCount>snapCount时拍摄快照，对于zk集群，因为强一致性，
                        //所有服务器几乎都是在同时达到这个条件，也就是同时触发拍摄快照，同时较长时间的磁盘io，
                        //这样一定会影响zk集群的整体性能，而且也不需要拍这么多快照。
                        //所以每个服务器计算一个随机值，大于这个值就拍快照，避免了所有服务器都同时拍摄快照
                        if (logCount > (snapCount / 2 + randRoll)) {
                            //达到拍快照条件
                            randRoll = r.nextInt(snapCount/2);
                            // roll the log
                            //滚动日志 没理解术语含义，看实现是如果流不为空，flush缓存数据到磁盘，然后将流对象置为null。
                            //理解：拍快照是对内存树拍快照，一定要在一个数据稳定的状态来拍，
                            //拍快照取的点是lastZxid，在拍之前，这个zxid的持久化数据一定要达到最终状态，持久化完成，并且不可再写数据。
                            //我们不可能在一个数据处于不稳定的时候来拍快照。
                            //所以rollLog()就是保证这一点，刷缓存，并将输出流置为Null，也就是流结束了，后面再记日志的时候会创建新的流对象。
                            //到这里，新的数据还是只存在于事务日志中，内存中没有，所以对外不可见。
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                //已经有快照线程在运行了
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                //构造快照线程并启动线程
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                //拍个快照
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // if this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        //1. 对leader来说，next为AckRequestProcessor
                        //2. 对follower来说，next为SendAckRequestProcessor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    //添加到待flush列表
                    toFlush.add(si);
                    if (toFlush.size() > 1000) {
                        //上面的追加写事务日志，是追加到缓冲区，没有执行flush操作，对事物文件来说，我们只是将文件头写入到磁盘了，
                        //文件内容等待将toFlush批量flush到磁盘。
                        //大于1000时，批量刷新，这里体现了批量写磁盘
                        //flush方法内部会执行nextProcessor.processRequest
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally{
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        //看这个最底层的逻辑，是将所有的输出流都flush，确保写入磁盘。
        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                //1. 对leader来说，next为AckRequestProcessor
                //2. 对follower来说，next为SendAckRequestProcessor
                nextProcessor.processRequest(i);
            }
        }

        //SendAckRequestProcessor实现了Flushable接口，
        //flush方法逻辑是将上面while循环里写到缓冲区的所有request响应flush到底层网络io，发给leader。
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        //还是入队
        queuedRequests.add(request);
    }

}
