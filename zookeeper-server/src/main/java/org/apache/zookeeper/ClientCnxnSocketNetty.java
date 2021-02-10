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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.NettyUtils;
import org.apache.zookeeper.common.X509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.common.X509Exception.SSLContextException;

/**
 * ClientCnxnSocketNetty implements ClientCnxnSocket abstract methods.
 * It's responsible for connecting to server, reading/writing network traffic and
 * being a layer between network data and higher level packets.
 */
public class ClientCnxnSocketNetty extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocketNetty.class);

    private final EventLoopGroup eventLoopGroup;
    private Channel channel;
    private CountDownLatch firstConnect;
    private ChannelFuture connectFuture;
    private final Lock connectLock = new ReentrantLock();
    private final AtomicBoolean disconnected = new AtomicBoolean();
    private final AtomicBoolean needSasl = new AtomicBoolean();
    private final Semaphore waitSasl = new Semaphore(0);

    private static final AtomicReference<ByteBufAllocator> TEST_ALLOCATOR =
            new AtomicReference<>(null);

    ClientCnxnSocketNetty(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        // Client only has 1 outgoing socket, so the event loop group only needs
        // a single thread.
        //优先使用epoll
        eventLoopGroup = NettyUtils.newNioOrEpollEventLoopGroup(1 /* nThreads */);
        initProperties();
    }

    /**
     * 生命周期图
     * lifecycles diagram:
     * <p/>
     * loop:
     * - try:
     * - - !isConnected()
     * - - - connect()
     * - - doTransport()
     * - catch:
     * - - cleanup()
     * close()
     * <p/>
     * Other non-lifecycle methods are in jeopardy getting a null channel
     * when calling in concurrency. We must handle it.
     */

    @Override
    boolean isConnected() {
        // Assuming that isConnected() is only used to initiate connection,
        // not used by some other connection status judgement.
        connectLock.lock();
        try {
            return channel != null || connectFuture != null;
        } finally {
            connectLock.unlock();
        }
    }

    private Bootstrap configureBootstrapAllocator(Bootstrap bootstrap) {
        ByteBufAllocator testAllocator = TEST_ALLOCATOR.get();
        if (testAllocator != null) {
            return bootstrap.option(ChannelOption.ALLOCATOR, testAllocator);
        } else {
            return bootstrap;
        }
    }

    @Override
    void connect(InetSocketAddress addr) throws IOException {
        firstConnect = new CountDownLatch(1);

        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NettyUtils.nioOrEpollSocketChannel())
                .option(ChannelOption.SO_LINGER, -1)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ZKClientPipelineFactory(addr.getHostString(), addr.getPort()));
        bootstrap = configureBootstrapAllocator(bootstrap);
        bootstrap.validate();

        connectLock.lock();
        try {
            connectFuture = bootstrap.connect(addr);
            connectFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    // this lock guarantees that channel won't be assigned after cleanup().
                    boolean connected = false;
                    connectLock.lock();
                    try {
                        if (!channelFuture.isSuccess()) {
                            LOG.info("future isn't success, cause:", channelFuture.cause());
                            return;
                        } else if (connectFuture == null) {
                            LOG.info("connect attempt cancelled");
                            // If the connect attempt was cancelled but succeeded
                            // anyway, make sure to close the channel, otherwise
                            // we may leak a file descriptor.
                            channelFuture.channel().close();
                            return;
                        }
                        // setup channel, variables, connection, etc.
                        channel = channelFuture.channel();

                        disconnected.set(false);
                        //建立连接以后initialized置为false
                        initialized = false;
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;

                        sendThread.primeConnection();
                        updateNow();
                        updateLastSendAndHeard();

                        if (sendThread.tunnelAuthInProgress()) {
                            waitSasl.drainPermits();
                            needSasl.set(true);
                            sendPrimePacket();
                        } else {
                            needSasl.set(false);
                        }
                        connected = true;
                    } finally {
                        connectFuture = null;
                        connectLock.unlock();
                        if (connected) {
                            LOG.info("channel is connected: {}", channelFuture.channel());
                        }
                        // need to wake on connect success or failure to avoid
                        // timing out ClientCnxn.SendThread which may be
                        // blocked waiting for first connect in doTransport().
                        wakeupCnxn();
                        firstConnect.countDown();
                    }
                }
            });
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    void cleanup() {
        connectLock.lock();
        try {
            if (connectFuture != null) {
                connectFuture.cancel(false);
                connectFuture = null;
            }
            if (channel != null) {
                channel.close().syncUninterruptibly();
                channel = null;
            }
        } finally {
            connectLock.unlock();
        }
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            if (p == WakeupPacket.getInstance()) {
                iter.remove();
            }
        }
    }

    @Override
    void close() {
        eventLoopGroup.shutdownGracefully();
    }

    @Override
    void saslCompleted() {
        needSasl.set(false);
        waitSasl.release();
    }

    @Override
    void connectionPrimed() {
    }

    @Override
    void packetAdded() {
        // NO-OP. Adding a packet will already wake up a netty connection
        // so we don't need to add a dummy packet to the queue to trigger
        // a wake-up.
    }

    @Override
    void onClosing() {
        firstConnect.countDown();
        wakeupCnxn();
        LOG.info("channel is told closing");
    }

    private void wakeupCnxn() {
        if (needSasl.get()) {
            waitSasl.release();
        }
        outgoingQueue.add(WakeupPacket.getInstance());
    }

    @Override
    void doTransport(int waitTimeOut,
                     List<Packet> pendingQueue,
                     ClientCnxn cnxn)
            throws IOException, InterruptedException {
        try {
            //CountDownLatch 阻塞等待完成连接建立
            if (!firstConnect.await(waitTimeOut, TimeUnit.MILLISECONDS)) {
                //超时未建立连接，连接失败
                return;
            }
            Packet head = null;
            if (needSasl.get()) {
                if (!waitSasl.tryAcquire(waitTimeOut, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } else {
                //从出站队列中移除请求包
                head = outgoingQueue.poll(waitTimeOut, TimeUnit.MILLISECONDS);
            }
            // check if being waken up on closing.
            if (!sendThread.getZkState().isAlive()) {
                // adding back the packet to notify of failure in conLossPacket().
                addBack(head);
                return;
            }
            // channel disconnection happened
            if (disconnected.get()) {
                addBack(head);
                throw new EndOfStreamException("channel for sessionid 0x"
                        + Long.toHexString(sessionId)
                        + " is lost");
            }
            if (head != null) {
                //发送请求，会将outgoingQueue的数据清空，都发出去，才会返回
                doWrite(pendingQueue, head, cnxn);
            }
        } finally {
            updateNow();
        }
    }

    private void addBack(Packet head) {
        if (head != null && head != WakeupPacket.getInstance()) {
            outgoingQueue.addFirst(head);
        }
    }

    /**
     * Sends a packet to the remote peer and flushes the channel.
     * @param p packet to send.
     * @return a ChannelFuture that will complete when the write operation
     *         succeeds or fails.
     */
    private ChannelFuture sendPktAndFlush(Packet p) {
        return sendPkt(p, true);
    }

    /**
     * Sends a packet to the remote peer but does not flush() the channel.
     * @param p packet to send.
     * @return a ChannelFuture that will complete when the write operation
     *         succeeds or fails.
     */
    private ChannelFuture sendPktOnly(Packet p) {
        return sendPkt(p, false);
    }

    // Use a single listener instance to reduce GC
    private final GenericFutureListener<Future<Void>> onSendPktDoneListener = f -> {
        if (f.isSuccess()) {
            sentCount.getAndIncrement();
        }
    };

    private ChannelFuture sendPkt(Packet p, boolean doFlush) {
        // Assuming the packet will be sent out successfully. Because if it fails,
        // the channel will close and clean up queues.
        p.createBB();
        updateLastSend();
        final ByteBuf writeBuffer = Unpooled.wrappedBuffer(p.bb);
        final ChannelFuture result = doFlush
                ? channel.writeAndFlush(writeBuffer)
                : channel.write(writeBuffer);
        result.addListener(onSendPktDoneListener);
        return result;
    }

    private void sendPrimePacket() {
        // assuming the first packet is the priming packet.
        sendPktAndFlush(outgoingQueue.remove());
    }

    /**
     * doWrite handles writing the packets from outgoingQueue via network to server.
     * 按照从头到尾的顺序，循环从outgoingQueue队列中取数据，生成事务id，放到pendingQueue，然后写channel，
     * 把outgoingQueue都取空了之后，才会退出循环，最后调一次flush。
     * 按照这个顺序和单线程的写法，从packet被放到outgoingQueue中以后，后面都是按照从头到尾的顺序来的，也就是入队的先后顺序。
     * 按这个顺序放到pendingQueue，然后按这个顺序写channel，发送到服务器。服务器收到反序列化后，也是按照发送的先后顺序。
     * 因为是同一个服务器peer，经过处理器链，会按照先后顺序入队，也会按照这个顺序取出处理，就算是到了多线程并发的处理器，
     * 也是按照客户端sessionId取模选择执行线程，同一个客户端的请求交给同一个线程处理，也是按照先后顺序处理。
     * 所以也会按照请求的先后顺序，写响应到对应客户端。到了客户端，按照从头到尾的顺序直接从pendingQueue取packet，应该是一一匹配的。
     * 如果不匹配，看下他的处理逻辑，好像是抛异常。
     */
    private void doWrite(List<Packet> pendingQueue, Packet p, ClientCnxn cnxn) {
        updateNow();
        //是否有数据包被发送了
        boolean anyPacketsSent = false;
        while (true) {
            if (p != WakeupPacket.getInstance()) {
                //不是WakeupPacket
                if ((p.requestHeader != null) &&
                        (p.requestHeader.getType() != ZooDefs.OpCode.ping) &&
                        (p.requestHeader.getType() != ZooDefs.OpCode.auth)) {
                    //不是心跳 不是ACL
                    //生成事务id
                    p.requestHeader.setXid(cnxn.getXid());
                    synchronized (pendingQueue) {
                        //pendingQueue是一个LinkedList，非线程安全
                        //追加到尾部，等待匹配服务端响应数据
                        pendingQueue.add(p);
                    }
                }
                //channel write 但不flush
                sendPktOnly(p);
                //标记有数据包被发送了
                anyPacketsSent = true;
            }

            //是WakeupPacket 或者 上面写数据后继续执行
            if (outgoingQueue.isEmpty()) {
                //相当于outgoingQueue队列中只有一个WakeupPacket
                //或者 队列里面的数据都被写到channel里面，发送了，队列为空
                //因为只有这里能跳出循环，所以一定要把outgoingQueue里面的数据都发送完，才会退出。
                break;
            }

            //outgoingQueue非空，有其他类型待发送的请求数据
            //移除队列头
            p = outgoingQueue.remove();
        }

        //zk的作者真逗，哈哈
        // TODO: maybe we should flush in the loop above every N packets/bytes?
        // But, how do we determine the right value for N ...
        if (anyPacketsSent) {
            //有数据包被写入channel，这里调一次flush
            channel.flush();
        }
    }

    @Override
    void sendPacket(ClientCnxn.Packet p) throws IOException {
        if (channel == null) {
            throw new IOException("channel has been closed");
        }
        sendPktAndFlush(p);
    }

    @Override
    SocketAddress getRemoteSocketAddress() {
        Channel copiedChanRef = channel;
        return (copiedChanRef == null) ? null : copiedChanRef.remoteAddress();
    }

    @Override
    SocketAddress getLocalSocketAddress() {
        Channel copiedChanRef = channel;
        return (copiedChanRef == null) ? null : copiedChanRef.localAddress();
    }

    @Override
    void testableCloseSocket() throws IOException {
        Channel copiedChanRef = channel;
        if (copiedChanRef != null) {
            copiedChanRef.disconnect().awaitUninterruptibly();
        }
    }


    // *************** <END> CientCnxnSocketNetty </END> ******************
    private static class WakeupPacket {
        private static final Packet instance = new Packet(null, null, null, null, null);

        protected WakeupPacket() {
            // Exists only to defeat instantiation.
        }

        public static Packet getInstance() {
            return instance;
        }
    }

    /**
     * ZKClientPipelineFactory is the netty pipeline factory for this netty
     * connection implementation.
     */
    private class ZKClientPipelineFactory extends ChannelInitializer<SocketChannel> {
        private SSLContext sslContext = null;
        private SSLEngine sslEngine = null;
        private String host;
        private int port;

        public ZKClientPipelineFactory(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            if (clientConfig.getBoolean(ZKClientConfig.SECURE_CLIENT)) {
                initSSL(pipeline);
            }
            pipeline.addLast("handler", new ZKClientHandler());
        }

        // The synchronized is to prevent the race on shared variable "sslEngine".
        // Basically we only need to create it once.
        private synchronized void initSSL(ChannelPipeline pipeline) throws SSLContextException {
            if (sslContext == null || sslEngine == null) {
                try (X509Util x509Util = new ClientX509Util()) {
                    sslContext = x509Util.createSSLContext(clientConfig);
                    sslEngine = sslContext.createSSLEngine(host, port);
                    sslEngine.setUseClientMode(true);
                }
            }
            pipeline.addLast("ssl", new SslHandler(sslEngine));
            LOG.info("SSL handler added for channel: {}", pipeline.channel());
        }
    }

    /**
     * ZKClientHandler is the netty handler that sits in netty upstream last
     * place. It mainly handles read traffic and helps synchronize connection state.
     */
    private class ZKClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
        AtomicBoolean channelClosed = new AtomicBoolean(false);

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("channel is disconnected: {}", ctx.channel());
            cleanup();
        }

        /**
         * netty handler has encountered problems. We are cleaning it up and tell outside to close
         * the channel/connection.
         */
        private void cleanup() {
            if (!channelClosed.compareAndSet(false, true)) {
                return;
            }
            disconnected.set(true);
            onClosing();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
            //收到服务端响应
            //更新当前相对时间
            updateNow();
            while (buf.isReadable()) {
                if (incomingBuffer.remaining() > buf.readableBytes()) {
                    //剩余的响应数据长度不足以填满byteBuffer
                    int newLimit = incomingBuffer.position()
                            + buf.readableBytes();
                    //计算并更新limit的值
                    incomingBuffer.limit(newLimit);
                }

                //把数据读到incomingBuffer中
                buf.readBytes(incomingBuffer);
                incomingBuffer.limit(incomingBuffer.capacity());

                if (!incomingBuffer.hasRemaining()) {
                    //incomingBuffer满了
                    //转为读模式
                    incomingBuffer.flip();
                    if (incomingBuffer == lenBuffer) {
                        //获取可以读取的数据长度，然后创建新的byteBuffer
                        recvCount.getAndIncrement();
                        readLength();
                    } else if (!initialized) {
                        //和服务端建立连接后，initialized会被置为false
                        //这里相当于建立连接后的第一次读取服务端响应数据，读到的应该是建立连接的结果，比如服务器是否是readOnly、会话过期等
                        readConnectResult();
                        lenBuffer.clear();
                        //换回长度buffer
                        incomingBuffer = lenBuffer;
                        //客户端连接初始化完成
                        initialized = true;
                        updateLastHeard();
                    } else {
                        //处理服务端响应数据
                        sendThread.readResponse(incomingBuffer);
                        lenBuffer.clear();
                        incomingBuffer = lenBuffer;
                        //更新心跳
                        updateLastHeard();
                    }
                }
            }
            wakeupCnxn();
            // Note: SimpleChannelInboundHandler releases the ByteBuf for us
            // so we don't need to do it.
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("Exception caught", cause);
            cleanup();
        }
    }

    /**
     * Sets the test ByteBufAllocator. This allocator will be used by all
     * future instances of this class.
     * It is not recommended to use this method outside of testing.
     * @param allocator the ByteBufAllocator to use for all netty buffer
     *                  allocations.
     */
    static void setTestAllocator(ByteBufAllocator allocator) {
        TEST_ALLOCATOR.set(allocator);
    }

    /**
     * Clears the test ByteBufAllocator. The default allocator will be used
     * by all future instances of this class.
     * It is not recommended to use this method outside of testing.
     */
    static void clearTestAllocator() {
        TEST_ALLOCATOR.set(null);
    }
}
