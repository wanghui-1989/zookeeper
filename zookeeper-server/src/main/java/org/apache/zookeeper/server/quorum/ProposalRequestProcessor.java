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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 * 该RequestProcessor只是将请求转发到AckRequestProcessor和SyncRequestProcessor。
 *
 * Proposal:提案
 *
 * TODO 貌似只有Leader才有执行这个处理器？？？
 *
 * 主要逻辑：
 *  前一个处理器PrepRequestProcessor已经将请求封装好了，到这里其实就是应该由leader发起提案了（对写操作来说）
 *  这个处理器将请求分成两种进行处理，
 *   一个是Follower转发过来的"sync"同步请求。
 *   另一个是剩下的其他请求,主要是其他learner server端转过来的写请求，以及client的读请求。
 *
 * 1.先说如何处理Follower转发过来的"sync"同步请求：
 *   1.zk不能保证每个服务实例在每个时间都具有相同的ZooKeeper数据视图。由于网络延迟之类的因素，
 *     一个客户端可能会在另一客户端收到更改通知之前执行更新。考虑两个客户端A和B的情况，
 *     如果客户端A将znode/a的值从0设置为1，然后告诉客户端B读取/a，则客户端B可能读取旧值0，具体取决于连的哪个服务器。
 *     如果客户端A和客户端B读取相同的值很重要，则客户端B应该在执行读取之前从ZooKeeper API方法中调用sync()方法。
 *     sync是使得client当前连接着的ZooKeeper服务器，和ZooKeeper的Leader节点同步（sync）一下数据。
 *     用法一般是同一线程串行执行，先调 zookeeper.sync("关注的路径path","可阻塞的回调对象","回调上下文对象")，
 *     调用完后，会再调用"可阻塞的回调对象.await等阻塞方法"，等待Leader和当前Follower数据同步完成，返回响应，
 *     然后就可以调用zookeeper.getData("关注的路径path")了，可以保证在该路径的数据上，获取到和Leader一致的视图。
 *     具体可以参考org.apache.zookeeper.cli.SyncCommand#exec()和org.apache.zookeeper.server.quorum
 *     .EphemeralNodeDeletionTest#testEphemeralNodeDeletion()。
 *   2.当follower收到到客户端发来的sync请求时，会将这个请求添加到一个pendingSyncs队列里，然后将这个请求发送给leader，
 *     直到收到leader的Leader.SYNC响应消息时，才将这个请求从pendingSyncs队列里移除，并commit这个请求。
 *   3.当Leader收到一个sync请求时，如果leader当前没有待commit的决议，那么leader会立即发送一个Leader.SYNC消息给follower。
 *     否则，leader会等到当前最后一个待commit的决议完成后，再发送Leader.SYNC消息给Follower。
 *   4.其实这里面有一个隐含的逻辑，leader和follower之间的消息通信，是严格按顺序来发送的（TCP保证），
 *     因此，当follower接收到Leader.SYNC消息时，
 *     说明follower也一定接收到了leader之前（在leader接收到sync请求之前）发送的所有提案或者commit消息。
 *     这样就可以确保follower内存中的数据和leader是同步的了。客户端就能从连接的follower读取到最新的数据了。
 *
 *
 * 2.剩下的其他请求，主要是其他learner server端转过来的写请求，以及client的读请求。
 *   1.交个下一个处理器执行，即CommitProcessor
 *
 *
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
        LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;
    //对leader来说，nextProcessor为CommitProcessor
    RequestProcessor nextProcessor;

    //SyncRequestProcessor -> AckRequestProcessor
    //这个SyncRequestProcessor会作为一个独立的线程启动运行
    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        //构造对象后，就会触发该方法，启动syncProcessor线程
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         *
         */

        if (request instanceof LearnerSyncRequest){
            //Follower转发过来的sync同步请求，处理逻辑见类头部注释
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
            //剩下的请求，主要是其他learner server端转过来的写请求，以及client直连的读写请求。
            //交给下一个处理器执行，即CommitProcessor
            nextProcessor.processRequest(request);
            if (request.getHdr() != null) {
                // We need to sync and get consensus on any transactions
                //我们需要同步并就任何事务达成共识
                try {
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
