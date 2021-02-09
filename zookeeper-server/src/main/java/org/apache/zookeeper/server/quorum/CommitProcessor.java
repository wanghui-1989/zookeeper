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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.WorkerService;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 *
 * 内部引入线程池WorkerService workerPool。根据请求zxid取模找到对应的固定线程，执行请求，保证同一个zxid的请求被串行执行。
 *
 * 该RequestProcessor将传入的提交请求与本地提交的请求进行匹配。
 * 诀窍是，本地提交的更改系统状态的请求将作为传入的提交请求返回，因此我们需要将它们匹配。
 * CommitProcessor是多线程的。线程之间的通信是通过队列，原子和在处理器上同步的wait / notifyAll处理的。
 * CommitProcessor充当网关，用于允许请求继续处理管道的其余部分。它将允许许多读取请求，
 * 但只有一个写入请求可以同时进行，从而确保以事务ID顺序处理写入请求。
 * -1个提交处理器主线程，该线程监视请求队列，并根据工作线程的sessionId将请求分配给工作线程，
 *   以便始终将特定会话的读写请求分配给同一线程（因此保证按顺序运行）。
 * -0-N个工作线程，对请求运行其余的请求处理器管道。如果配置了0个工作线程，
 *   则主要提交处理器线程将直接运行管道。典型的（默认）线程计数是：在32核计算机上，1个提交处理器线程和32个工作线程。
 * 多线程约束：
 * -每个会话的请求必须按顺序处理。
 * -写请求必须以zxid顺序处理
 * -必须确保一个会话中的两次写入之间没有竞争条件，否则会触发另一会话中的读取请求设置监视
 * 当前的实现通过简单地不允许任何读请求与写请求并行处理来解决第三种约束。
 * 我们使用LinkedBlockingQueue<Request> queuedRequests将并行多请求，转为串行排序的请求。
 *
 *
 * 说下它的流程：
 *    1. 对服务器来说，只会启动一个CommitProcessor线程。
 *    2. 在线程第一次运行时，因为queuedRequests和committedRequests都为空，所以线程阻塞wait。
 *    3. 然后上一个处理器调用CommitProcessor.processRequest()提交请求到queuedRequests队列中。
 *    4. 因为此时是第一个请求，无待commit请求，所以会调用notifyAll唤醒阻塞的所有线程。
 *    5. 线程唤醒后，因为queuedRequests不为空，所以跳出while循环，向下走。此时下一个while循环为true，因为没有待commit或者正在commit的请求，而且从queuedRequests拉数据不为空。
 *    6. 判断这个请求是不是需要被commit的请求，即写请求。
 *       1. 如果是读请求，更新正在处理请求的计数numRequestsProcessing+1，根据请求的sessionId对线程数组取余，拿到执行线程，封装成任务，提交给该线程，线程内操作就是将该读请求提交给下一个执行器。即调用nextProcessor.processRequest(request);一般就是走到FinalRequestProcessor，会根据路径取对应内存DataTree的node节点，将数据封装成响应，写回给客户端。回到CommitProcessor后会执行finally块，里面会将currentlyCommitting置为null，将正在处理请求的计数numRequestsProcessing-1。相当于该numRequestsProcessing最大值为1。
 *       2. 如果是写请求，则set为待commit请求，赋值给nextPending变量。因为有了待commit请求，所以再走while循环条件不成立，跳出while，向下走。
 *    7. 因为committedRequests为空，所以if条件不成立，走回while循环，继续阻塞wait。
 *    8. 假如后来再有一个写请求入队queuedRequests。根据上面说的逻辑，会赋值给nextPending变量。
 *    9. 假如leader对写的请求投票结束，确定要commit该数据，会给Follower发送COMMIT,给Observer发送INFORM命令。处理器链会调用CommitProcessor.commit，将待commit请求入队committedRequests。调用notifyAll唤醒阻塞线程。
 *    10. 此时因为committedRequests不为空，并且也没有正在commit的请求，所以跳出while，向下走。
 *    11. 从committedRequests出队该已确定要被提交的请求，set为正在commit的请求，即赋值给currentlyCommitting。然后将待commit变量置为null，即nextPending为null。
 *    12. 更新正在处理请求的计数numRequestsProcessing+1，根据请求的sessionId对线程数组取余，拿到执行线程，封装成任务，提交给该线程，线程内操作就是将该读请求提交给下一个执行器。即调用nextProcessor.processRequest(request);一般就是走到FinalRequestProcessor。FinalRequestProcessor会将写请求的数据，写到内存DataTree上，写成功响应给client。
 *    13. 回到CommitProcessor后会执行finally块，里面会将currentlyCommitting置为null，将正在处理请求的计数numRequestsProcessing-1。相当于该numRequestsProcessing最大值为1。
 *    14. 假如在有一个写请求处于待commit状态，且CommitProcessor线程处于wait状态，此时有一个读请求或者写请求被上一个处理器塞到了queuedRequests中，因为CommitProcessor线程阻塞等待leader的提交命令来唤醒，所以在没有leader的commit命令之前，都是阻塞的，不会从queuedRequests队列中去拿后面的读或者写请求。相当于没有写的情况下，可以同时有多个读，只要有写的情况下，只能有一个写。很类似于读写锁。
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS =
        "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT =
        "zookeeper.commitProcessor.shutdownTimeout";

    /**
     * Requests that we are holding until the commit comes in.
     * 在提交之前，我们一直保留的请求。
     */
    protected final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();

    /**
     * Requests that have been committed.
     * 已提交的请求。即已经投票通过，leader确定可以提交的请求。
     */
    protected final LinkedBlockingQueue<Request> committedRequests =
        new LinkedBlockingQueue<Request>();

    /**
     * Request for which we are currently awaiting a commit
     * 等待leader commit命令的请求
     */
    protected final AtomicReference<Request> nextPending =
        new AtomicReference<Request>();
    /** 当前正在被提交的请求 Request currently being committed (ie, sent off to next processor) */
    private final AtomicReference<Request> currentlyCommitting =
        new AtomicReference<Request>();

    /** The number of requests currently being processed */
    protected AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    //对leader来说应该是Leader.ToBeAppliedRequestProcessor
    RequestProcessor nextProcessor;

    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
                           boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    private boolean isWaitingForCommit() {
        return nextPending.get() != null;
    }

    private boolean isProcessingCommit() {
        return currentlyCommitting.get() != null;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;
            case OpCode.sync:
                return matchSyncs;    
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    @Override
    public void run() {
        Request request;
        try {
            while (!stopped) {
                synchronized(this) {
                    //未停止
                    //并且 （
                    //        （待处理请求队列为空  或者  有等待commit请求  或者 有正在执行commit请求）
                    //  并且 （已提交的请求队列为空  或者 正在处理的请求数量>0  ）
                    // ）
                    while (
                        !stopped &&
                        ((queuedRequests.isEmpty() || isWaitingForCommit() || isProcessingCommit()) &&
                         (committedRequests.isEmpty() || isProcessingRequest()))) {
                        //总之就是队列为空 或者 有待提交的写请求，线程阻塞等着
                        //队列为空阻塞等，有正在处理的写请求时，不能处理其他读写请求。
                        wait();
                    }
                }

                /*
                 * Processing queuedRequests: Process the next requests until we
                 * find one for which we need to wait for a commit. We cannot
                 * process a read request while we are processing write request.
                 * 处理queuedRequests：处理下一个请求，直到找到需要等待提交的请求为止。 我们在处理写请求时无法处理读请求。
                 */
                while (!stopped && !isWaitingForCommit() &&
                       !isProcessingCommit() &&
                       (request = queuedRequests.poll()) != null) {
                    //无正在处理的写请求，并且queuedRequests有数据
                    if (needCommit(request)) {
                        //是创建、删除、修改等写请求，需要提交的请求
                        //将请求set为正在处理的请求
                        nextPending.set(request);
                    } else {
                        //读数据等不需要提交的读请求
                        //根据请求的zxid对ArrayList<ExecutorService>取模，拿到对应的固定线程执行器
                        //执行请求，保证同一个zxid的请求是被同一个线程处理器处理，顺序执行，内部逻辑就是
                        //拿到下一个processer，调用processer.processRequest
                        sendToNextProcessor(request);
                    }
                }

                /*
                 * Processing committedRequests: check and see if the commit
                 * came in for the pending request. We can only commit a
                 * request when there is no other request being processed.
                 *
                 * 处理已确认要提交的请求，将请求数据应用到内存树。
                 */
                processCommitted();
            }
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    /*
     * Separated this method from the main run loop
     * for test purposes (ZOOKEEPER-1863)
     *
     */
    protected void processCommitted() {
        Request request;

        if (!stopped && !isProcessingRequest() &&
                (committedRequests.peek() != null)) {

            /*
             * ZOOKEEPER-1863: continue only if there is no new request
             * waiting in queuedRequests or it is waiting for a
             * commit. 
             */
            if ( !isWaitingForCommit() && !queuedRequests.isEmpty()) {
                return;
            }
            request = committedRequests.poll();

            /*
             * We match with nextPending so that we can move to the
             * next request when it is committed. We also want to
             * use nextPending because it has the cnxn member set
             * properly.
             *
             * 我们与nextPending匹配，以便在提交后可以移至下一个请求。
             * 我们还想使用nextPending，因为它已正确设置了cnxn成员。
             */
            Request pending = nextPending.get();
            if (pending != null &&
                pending.sessionId == request.sessionId &&
                pending.cxid == request.cxid) {
                // we want to send our version of the request.
                // the pointer to the connection in the request
                pending.setHdr(request.getHdr());
                pending.setTxn(request.getTxn());
                pending.zxid = request.zxid;
                // Set currentlyCommitting so we will block until this
                // completes. Cleared by CommitWorkRequest after
                // nextProcessor returns.
                currentlyCommitting.set(pending);
                nextPending.set(null);
                //交给下一个处理器，提交数据到内存树。
                sendToNextProcessor(pending);
            } else {
                // this request came from someone else so just
                // send the commit packet
                currentlyCommitting.set(request);
                sendToNextProcessor(request);
            }
        }      
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(
            ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(
            ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with "
                 + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                 + " worker threads.");
        if (workerPool == null) {
            workerPool = new WorkerService(
                "CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor,"
                          + " unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                nextProcessor.processRequest(request);
            } finally {
                // If this request is the commit request that was blocking
                // the processor, clear.
                currentlyCommitting.compareAndSet(request, null);

                /*
                 * Decrement outstanding request count. The processor may be
                 * blocked at the moment because it is waiting for the pipeline
                 * to drain. In that case, wake it up if there are pending
                 * requests.
                 */
                if (numRequestsProcessing.decrementAndGet() == 0) {
                    if (!queuedRequests.isEmpty() ||
                        !committedRequests.isEmpty()) {
                        wakeup();
                    }
                }
            }
        }
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    synchronized private void wakeup() {
        notifyAll();
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }
        committedRequests.add(request);
        if (!isProcessingCommit()) {
            wakeup();
        }
    }

    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        queuedRequests.add(request);
        if (!isWaitingForCommit()) {
            wakeup();
        }
    }

    private void halt() {
        stopped = true;
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
