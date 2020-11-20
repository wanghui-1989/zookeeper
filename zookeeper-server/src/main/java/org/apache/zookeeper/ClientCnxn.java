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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ClientCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    /**
     *  ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;

    static class AuthData {
        AuthData(String scheme, byte data[]) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte data[];
    }

    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     * 写入channel后，放入该队列，等待响应返回
     */
    private final LinkedList<Packet> pendingQueue = new LinkedList<Packet>();

    /**
     * These are the packets that need to be sent.
     * 需要被发送的信息队列
     */
    private final LinkedBlockingDeque<Packet> outgoingQueue = new LinkedBlockingDeque<Packet>();

    private int connectTimeout;

    /**
     * The timeout in ms the client negotiated with the server. This is the
     * "real" timeout, not the timeout request by the client (which may have
     * been increased/decreased by the server which applies bounds to this
     * value.
     */
    private volatile int negotiatedSessionTimeout;

    private int readTimeout;

    private final int sessionTimeout;

    private final ZooKeeper zooKeeper;

    private final ClientWatchManager watcher;

    private long sessionId;

    private byte sessionPasswd[] = new byte[16];

    /**
     * If true, the connection is allowed to go to r-o mode. This field's value
     * is sent, besides other data, during session creation handshake. If the
     * server on the other side of the wire is partitioned it'll accept
     * read-only clients only.
     * 如果为true，则允许连接进入r-o模式。
     * 在会话创建握手期间，除其他数据外，还将发送该字段的值。
     * 如果线路另一端的服务器已分区，它将仅接受只读客户端。
     */
    private boolean readOnly;

    final String chrootPath;

    final SendThread sendThread;

    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    private volatile boolean closing = false;
    
    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    private final HostProvider hostProvider;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     *
     * 首次建立与r/w服务器的连接时设置为true，此后从未改变。
     * 用于处理没有sessionId的客户端连接到只读服务器时的情况。
     * 这样的客户端从只读服务器接收“伪”sessionId，但是该sessionId对于其他服务器无效。
     * 因此，当此类客户端找到一个r/w服务器时，它将在连接握手期间发送0而不是伪造的sessionId，并建立新的有效会话。
     * 如果该字段为假（这意味着我们之前从未见过R/W服务器），非零sessionId为假，否则为有效。
     */
    volatile boolean seenRwServerBefore = false;


    public ZooKeeperSaslClient zooKeeperSaslClient;

    private final ZKClientConfig clientConfig;
    /**
     * If any request's response in not received in configured requestTimeout
     * then it is assumed that the response packet is lost.
     */
    private long requestTimeout;

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb
            .append("sessionid:0x").append(Long.toHexString(getSessionId()))
            .append(" local:").append(local)
            .append(" remoteserver:").append(remote)
            .append(" lastZxid:").append(lastZxid)
            .append(" xid:").append(xid)
            .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
            .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
            .append(" queuedpkts:").append(outgoingQueue.size())
            .append(" pendingresp:").append(pendingQueue.size())
            .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     */
    static class Packet {
        RequestHeader requestHeader;

        ReplyHeader replyHeader;

        Record request;

        Record response;

        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) **/
        String serverPath;

        boolean finished;

        AsyncCallback cb;

        Object ctx;

        WatchRegistration watchRegistration;

        public boolean readOnly;

        WatchDeregistration watchDeregistration;

        /** Convenience ctor */
        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response,
                 watchRegistration, false);
        }

        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration, boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                LOG.warn("Ignoring unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly)
            throws IOException {
        this(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher,
             clientCnxnSocket, 0, new byte[16], canBeReadOnly);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly) {
        this.zooKeeper = zooKeeper;
        this.watcher = watcher;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.sessionTimeout = sessionTimeout;
        this.hostProvider = hostProvider;
        this.chrootPath = chrootPath;

        connectTimeout = sessionTimeout / hostProvider.size();
        readTimeout = sessionTimeout * 2 / 3;
        readOnly = canBeReadOnly;

        //传入客户端socket，构建1个发送线程
        sendThread = new SendThread(clientCnxnSocket);
        //构建1个事件线程
        eventThread = new EventThread();
        this.clientConfig=zooKeeper.getClientConfig();
        initRequestTimeout();
    }

    public void start() {
        //客户端启动两个线程
        sendThread.start();
        eventThread.start();
    }

    private Object eventOfDeath = new Object();

    private static class WatcherSetEventPair {
        private final Set<Watcher> watchers;
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }
    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().
            replaceAll("-EventThread", "");
        return name + suffix;
    }


    /**
     * 事件线程
     * 内部有一个LinkedBlockingQueue，存储多种类型的对象，
     * 有的是被触发的一个事件和对应的多个观察者(1:N)会封装成一对,即WatcherSetEventPair，加到队列尾部，
     * 有的是毒丸，有的是LocalCallback等等。
     * 1个EventThread不断地从waitingEvents队列中取出，遍历、同步调用执行事件处理逻辑。
     * 1个客户端ClientCnxn对象，内部只有1个EventThread和1个sendThread。
     *
     * 如果遇到当前客户端会话关闭等事件时，会入队一个毒丸eventOfDeath，
     * EventThread会在执行完所有waitingEvents队列中的任务之后，关闭线程，杀死自己。
     *
     * 操作1个队列：
     *  1. LinkedBlockingQueue<Object> waitingEvents 已触发的事件 和 监听对应事件的观察者 队列
     */
    class EventThread extends ZooKeeperThread {
        private final LinkedBlockingQueue<Object> waitingEvents =
            new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

       private volatile boolean wasKilled = false;
       private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        public void queueEvent(WatchedEvent event) {
            queueEvent(event, null);
        }

        private void queueEvent(WatchedEvent event,
                Set<Watcher> materializedWatchers) {
            if (event.getType() == EventType.None
                    && sessionState == event.getState()) {
                //因为多个线程会调用这个方法，可能别的线程已经调过，状态已经变更过了，
                //不需要再变更服务器状态了
                return;
            }
            //变更zk状态为事件中的zk状态
            sessionState = event.getState();
            final Set<Watcher> watchers;
            if (materializedWatchers == null) {
                //入参的观察者为空的话，就调用客户端的观察者管理器，判断之前注册的事件中，哪些观察者应该被触发调用。
                // materialize the watchers based on the event
                watchers = watcher.materialize(event.getState(),
                        event.getType(), event.getPath());
            } else {
                //入参给了应该被触发、调用的观察者，那就用给的这些
                watchers = new HashSet<Watcher>();
                watchers.addAll(materializedWatchers);
            }

            //组成对
            WatcherSetEventPair pair = new WatcherSetEventPair(watchers, event);
            // queue the pair (watch set & event) for later processing
            //加到EventThread的阻塞队列中，准备执行事件处理逻辑
            waitingEvents.add(pair);
        }

        public void queueCallback(AsyncCallback cb, int rc, String path,
                Object ctx) {
            waitingEvents.add(new LocalCallback(cb, rc, path, ctx));
        }

       @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
       public void queuePacket(Packet packet) {
          if (wasKilled) {
             //拿到过毒丸
             synchronized (waitingEvents) {
                if (isRunning) waitingEvents.add(packet); //还没有关闭eventThread，加到队列中
                else processEvent(packet); //已关闭线程，用当前线程执行事件
             }
          } else {
             // 正常逻辑，入队
             waitingEvents.add(packet);
          }
       }

        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        @Override
        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void run() {
           try {
              isRunning = true;
              while (true) {
                 Object event = waitingEvents.take();
                 if (event == eventOfDeath) {
                    //毒丸，找机会杀死自己。
                    wasKilled = true;
                 } else {
                    processEvent(event);
                 }
                 if (wasKilled)
                    synchronized (waitingEvents) {
                       if (waitingEvents.isEmpty()) {
                          //等到waitingEvents队列为空时，退出循环，线程停止运行
                          isRunning = false;
                          break;
                       }
                    }
              }
           } catch (InterruptedException e) {
              LOG.error("Event thread exiting due to interruption", e);
           }

            LOG.info("EventThread shut down for session: 0x{}",
                     Long.toHexString(getSessionId()));
        }

       private void processEvent(Object event) {
          try {
              if (event instanceof WatcherSetEventPair) {
                  // each watcher will process the event
                  WatcherSetEventPair pair = (WatcherSetEventPair) event;
                  for (Watcher watcher : pair.watchers) {
                      try {
                          watcher.process(pair.event);
                      } catch (Throwable t) {
                          LOG.error("Error while calling watcher ", t);
                      }
                  }
                } else if (event instanceof LocalCallback) {
                    LocalCallback lcb = (LocalCallback) event;
                    if (lcb.cb instanceof StatCallback) {
                        ((StatCallback) lcb.cb).processResult(lcb.rc, lcb.path,
                                lcb.ctx, null);
                    } else if (lcb.cb instanceof DataCallback) {
                        ((DataCallback) lcb.cb).processResult(lcb.rc, lcb.path,
                                lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ACLCallback) {
                        ((ACLCallback) lcb.cb).processResult(lcb.rc, lcb.path,
                                lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ChildrenCallback) {
                        ((ChildrenCallback) lcb.cb).processResult(lcb.rc,
                                lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof Children2Callback) {
                        ((Children2Callback) lcb.cb).processResult(lcb.rc,
                                lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof StringCallback) {
                        ((StringCallback) lcb.cb).processResult(lcb.rc,
                                lcb.path, lcb.ctx, null);
                    } else {
                        ((VoidCallback) lcb.cb).processResult(lcb.rc, lcb.path,
                                lcb.ctx);
                    }
                } else {
                  Packet p = (Packet) event;
                  int rc = 0;
                  String clientPath = p.clientPath;
                  if (p.replyHeader.getErr() != 0) {
                      rc = p.replyHeader.getErr();
                  }
                  if (p.cb == null) {
                      LOG.warn("Somehow a null cb got to EventThread!");
                  } else if (p.response instanceof ExistsResponse
                          || p.response instanceof SetDataResponse
                          || p.response instanceof SetACLResponse) {
                      StatCallback cb = (StatCallback) p.cb;
                      if (rc == 0) {
                          if (p.response instanceof ExistsResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((ExistsResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetDataResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetDataResponse) p.response)
                                              .getStat());
                          } else if (p.response instanceof SetACLResponse) {
                              cb.processResult(rc, clientPath, p.ctx,
                                      ((SetACLResponse) p.response)
                                              .getStat());
                          }
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetDataResponse) {
                      DataCallback cb = (DataCallback) p.cb;
                      GetDataResponse rsp = (GetDataResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getData(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetACLResponse) {
                      ACLCallback cb = (ACLCallback) p.cb;
                      GetACLResponse rsp = (GetACLResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getAcl(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null,
                                  null);
                      }
                  } else if (p.response instanceof GetChildrenResponse) {
                      ChildrenCallback cb = (ChildrenCallback) p.cb;
                      GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof GetChildren2Response) {
                      Children2Callback cb = (Children2Callback) p.cb;
                      GetChildren2Response rsp = (GetChildren2Response) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx, rsp
                                  .getChildren(), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null, null);
                      }
                  } else if (p.response instanceof CreateResponse) {
                      StringCallback cb = (StringCallback) p.cb;
                      CreateResponse rsp = (CreateResponse) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx,
                                  (chrootPath == null
                                          ? rsp.getPath()
                                          : rsp.getPath()
                                    .substring(chrootPath.length())));
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null);
                      }
                  } else if (p.response instanceof Create2Response) {
                	  Create2Callback cb = (Create2Callback) p.cb;
                      Create2Response rsp = (Create2Response) p.response;
                      if (rc == 0) {
                          cb.processResult(rc, clientPath, p.ctx,
                                  (chrootPath == null
                                          ? rsp.getPath()
                                          : rsp.getPath()
                                    .substring(chrootPath.length())), rsp.getStat());
                      } else {
                          cb.processResult(rc, clientPath, p.ctx, null, null);
                      }                   
                  } else if (p.response instanceof MultiResponse) {
                	  MultiCallback cb = (MultiCallback) p.cb;
                	  MultiResponse rsp = (MultiResponse) p.response;
                	  if (rc == 0) {
                		  List<OpResult> results = rsp.getResultList();
                		  int newRc = rc;
                		  for (OpResult result : results) {
                			  if (result instanceof ErrorResult
                					  && KeeperException.Code.OK.intValue() != (newRc = ((ErrorResult) result)
                					  .getErr())) {
                				  break;
                			  }
                		  }
                		  cb.processResult(newRc, clientPath, p.ctx, results);
                	  } else {
                		  cb.processResult(rc, clientPath, p.ctx, null);
                	  }
                  }  else if (p.cb instanceof VoidCallback) {
                      VoidCallback cb = (VoidCallback) p.cb;
                      cb.processResult(rc, clientPath, p.ctx);
                  }
              }
          } catch (Throwable t) {
              LOG.error("Caught unexpected throwable", t);
          }
       }
    }

    // @VisibleForTesting
    //收到服务端响应后的finally处理，一定执行
    protected void finishPacket(Packet p) {
        int err = p.replyHeader.getErr();
        if (p.watchRegistration != null) {
            p.watchRegistration.register(err);
        }
        // Add all the removed watch events to the event queue, so that the
        // clients will be notified with 'Data/Child WatchRemoved' event type.
        if (p.watchDeregistration != null) {
            Map<EventType, Set<Watcher>> materializedWatchers = null;
            try {
                materializedWatchers = p.watchDeregistration.unregister(err);
                for (Entry<EventType, Set<Watcher>> entry : materializedWatchers
                        .entrySet()) {
                    Set<Watcher> watchers = entry.getValue();
                    if (watchers.size() > 0) {
                        queueEvent(p.watchDeregistration.getClientPath(), err,
                                watchers, entry.getKey());
                        // ignore connectionloss when removing from local
                        // session
                        p.replyHeader.setErr(Code.OK.intValue());
                    }
                }
            } catch (KeeperException.NoWatcherException nwe) {
                p.replyHeader.setErr(nwe.code().intValue());
            } catch (KeeperException ke) {
                p.replyHeader.setErr(ke.code().intValue());
            }
        }

        if (p.cb == null) {
            //未注册回调
            synchronized (p) {
                //收到响应，通信完成
                p.finished = true;
                //唤醒所有阻塞线程，比如客户端同步发送请求，阻塞等待响应，此时p.response拿到响应，唤醒返回
                p.notifyAll();
            }
        } else {
            //注册了回调
            p.finished = true;
            //入队waitingEvents
            eventThread.queuePacket(p);
        }
    }

    void queueEvent(String clientPath, int err,
            Set<Watcher> materializedWatchers, EventType eventType) {
        KeeperState sessionState = KeeperState.SyncConnected;
        if (KeeperException.Code.SESSIONEXPIRED.intValue() == err
                || KeeperException.Code.CONNECTIONLOSS.intValue() == err) {
            sessionState = Event.KeeperState.Disconnected;
        }
        WatchedEvent event = new WatchedEvent(eventType, sessionState,
                clientPath);
        eventThread.queueEvent(event, materializedWatchers);
    }

    void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
        eventThread.queueCallback(cb, rc, path, ctx);
    }

    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch (state) {
        case AUTH_FAILED:
            p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
            break;
        case CLOSED:
            p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
            break;
        default:
            p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    private volatile long lastZxid;

    public long getLastZxid() {
        return lastZxid;
    }

    static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }
        
        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    private static class SessionTimeoutException extends IOException {
        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }
    }
    
    private static class SessionExpiredException extends IOException {
        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }
    }

    private static class RWServerFoundException extends IOException {
        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }
    }

    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     *
     * 发送线程
     * 负责从outgoingQueue拿取请求packet、发送packet，包括客户端的业务请求和心跳、ACL等。
     * 负责读取服务端响应，判断数据类型，比如ping的响应、心跳、ACL、业务、事件等
     * 如果是事件会调用事件线程eventThread入队事件。
     * 如果是业务数据会根据发送顺序匹配请求对象。
     *
     * 操作两个队列：
     *   1. LinkedBlockingDeque<Packet> outgoingQueue 待发送请求队列
     *   2. LinkedList<Packet> pendingQueue 已发送请求，等待响应返回，匹配对应的请求和响应，将响应set到Packet.response
     *
     */
    class SendThread extends ZooKeeperThread {
        private long lastPingSentNs;
        private final ClientCnxnSocket clientCnxnSocket;
        private Random r = new Random();
        private boolean isFirstConnect = true;

        //读取响应ByteBuffer
        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(
                    incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            replyHdr.deserialize(bbia, "header");
            if (replyHdr.getXid() == -2) {
                //ping心跳响应
                //客户端ping服务端的响应
                // -2 is the xid for pings
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got ping response for sessionid: 0x"
                            + Long.toHexString(sessionId)
                            + " after "
                            + ((System.nanoTime() - lastPingSentNs) / 1000000)
                            + "ms");
                }
                return;
            }
            if (replyHdr.getXid() == -4) {
                //zk的ACL权限认证结果
                // -4 is the xid for AuthPacket
                //权限认证失败
                if(replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    state = States.AUTH_FAILED;
                    //权限认证失败事件入队
                    eventThread.queueEvent( new WatchedEvent(Watcher.Event.EventType.None, 
                            Watcher.Event.KeeperState.AuthFailed, null) );
                    //入队毒丸
                    eventThread.queueEventOfDeath();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got auth sessionid:0x"
                            + Long.toHexString(sessionId));
                }
                return;
            }
            if (replyHdr.getXid() == -1) {
                //事件
                // -1 means notification
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got notification sessionid:0x"
                        + Long.toHexString(sessionId));
                }
                WatcherEvent event = new WatcherEvent();
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if(serverPath.compareTo(chrootPath)==0)
                        event.setPath("/");
                    else if (serverPath.length() > chrootPath.length())
                        event.setPath(serverPath.substring(chrootPath.length()));
                    else {
                    	LOG.warn("Got server path " + event.getPath()
                    			+ " which is too short for chroot path "
                    			+ chrootPath);
                    }
                }

                WatchedEvent we = new WatchedEvent(event);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got " + we + " for sessionid 0x"
                            + Long.toHexString(sessionId));
                }

                //入队waitingEvents
                eventThread.queueEvent( we );
                return;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (tunnelAuthInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia,"token");
                zooKeeperSaslClient.respondToServer(request.getToken(),
                  ClientCnxn.this);
                return;
            }

            //这里是业务响应了
            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got "
                            + replyHdr.getXid());
                }
                //移除头部第一个packet，匹配最新的响应
                //这里涉及到zk的消息顺序性保证了
                //因为连接的是同一台服务器，所以排除了不同服务器性能不同，导致计算时间不同的影响。
                //同一台服务器，单线程顺序执行，客户端先发送的请求，也会被服务端先收到，也会先计算出响应，先发给客户端。
                //相当于客户端有序发出请求，服务端按这个顺序处理请求，返回响应。
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             * 由于请求是按顺序发出的，因此收到的这个响应是第一个请求的响应。
             */
            try {
                //比较事务id，事务id不匹配，也就是请求和响应不匹配
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(
                            KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid "
                            + replyHdr.getXid() + " with err " +
                            + replyHdr.getErr() +
                            " expected Xid "
                            + packet.requestHeader.getXid()
                            + " for a packet with details: "
                            + packet );
                }

                //事务id匹配，就是请求和响应是匹配的
                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    //无错误，有响应报文，反序列化响应数据
                    packet.response.deserialize(bbia, "response");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading reply sessionid:0x"
                            + Long.toHexString(sessionId) + ", packet:: " + packet);
                }
            } finally {
                //收尾工作，一定执行
                finishPacket(packet);
            }
        }

        SendThread(ClientCnxnSocket clientCnxnSocket) {
            super(makeThreadName("-SendThread()"));
            //尝试连接中
            state = States.CONNECTING;
            this.clientCnxnSocket = clientCnxnSocket;
            //守护线程
            setDaemon(true);
        }

        // TODO: can not name this method getState since Thread.getState()
        // already exists
        // It would be cleaner to make class SendThread an implementation of
        // Runnable
        /**
         * Used by ClientCnxnSocket
         * 
         * @return
         */
        ZooKeeper.States getZkState() {
            return state;
        }

        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        /**
         * Setup session, previous watches, authentication.
         * prime：v. 填装
         * 完成建立连接后的操作，包括重置watch
         */
        void primeConnection() throws IOException {
            LOG.info("Socket connection established, initiating session, client: {}, server: {}",
                    clientCnxnSocket.getLocalSocketAddress(),
                    clientCnxnSocket.getRemoteSocketAddress());
            isFirstConnect = false;
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            ConnectRequest conReq = new ConnectRequest(0, lastZxid,
                    sessionTimeout, sessId, sessionPasswd);
            // We add backwards since we are pushing into the front
            // Only send if there's a pending watch
            // TODO: here we have the only remaining use of zooKeeper in
            // this class. It's to be eliminated!
            if (!clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET)) {
                //reconnect后，重置watch
                List<String> dataWatches = zooKeeper.getDataWatches();
                List<String> existWatches = zooKeeper.getExistWatches();
                List<String> childWatches = zooKeeper.getChildWatches();
                if (!dataWatches.isEmpty()
                        || !existWatches.isEmpty() || !childWatches.isEmpty()) {
                    Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                    Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                    Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                    long setWatchesLastZxid = lastZxid;

                    while (dataWatchesIter.hasNext()
                           || existWatchesIter.hasNext() || childWatchesIter.hasNext()) {
                        List<String> dataWatchesBatch = new ArrayList<String>();
                        List<String> existWatchesBatch = new ArrayList<String>();
                        List<String> childWatchesBatch = new ArrayList<String>();
                        int batchLength = 0;

                        // Note, we may exceed our max length by a bit when we add the last
                        // watch in the batch. This isn't ideal, but it makes the code simpler.
                        while (batchLength < SET_WATCHES_MAX_LENGTH) {
                            final String watch;
                            if (dataWatchesIter.hasNext()) {
                                watch = dataWatchesIter.next();
                                dataWatchesBatch.add(watch);
                            } else if (existWatchesIter.hasNext()) {
                                watch = existWatchesIter.next();
                                existWatchesBatch.add(watch);
                            } else if (childWatchesIter.hasNext()) {
                                watch = childWatchesIter.next();
                                childWatchesBatch.add(watch);
                            } else {
                                break;
                            }
                            batchLength += watch.length();
                        }

                        SetWatches sw = new SetWatches(setWatchesLastZxid,
                                                       dataWatchesBatch,
                                                       existWatchesBatch,
                                                       childWatchesBatch);
                        RequestHeader header = new RequestHeader(-8, OpCode.setWatches);
                        Packet packet = new Packet(header, new ReplyHeader(), sw, null, null);
                        //还是放到outgoingQueue
                        outgoingQueue.addFirst(packet);
                    }
                }
            }

            //zk权限认证，scheme:id:permission
            for (AuthData id : authInfo) {
                outgoingQueue.addFirst(new Packet(new RequestHeader(-4,
                        OpCode.auth), null, new AuthPacket(0, id.scheme,
                        id.data), null, null));
            }
            outgoingQueue.addFirst(new Packet(null, null, conReq,
                    null, null, readOnly));

            //连接初始化完成，修改nio状态为可读写
            clientCnxnSocket.connectionPrimed();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session establishment request sent on "
                        + clientCnxnSocket.getRemoteSocketAddress());
            }
        }

        //替换或者追加真实的根目录
        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        //ping心跳，只有请求头
        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            RequestHeader h = new RequestHeader(-2, OpCode.ping);
            //入队，outgoingQueue
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        private InetSocketAddress rwServerAddress = null;

        private final static int minPingRwTimeout = 100;

        private final static int maxPingRwTimeout = 60000;

        private int pingRwTimeout = minPingRwTimeout;

        // Set to true if and only if constructor of ZooKeeperSaslClient
        // throws a LoginException: see startConnect() below.
        private boolean saslLoginFailed = false;

        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;
            if(!isFirstConnect){
                try {
                    Thread.sleep(r.nextInt(1000));
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected exception", e);
                }
            }
            state = States.CONNECTING;

            String hostPort = addr.getHostString() + ":" + addr.getPort();
            MDC.put("myid", hostPort);
            setName(getName().replaceAll("\\(.*\\)", "(" + hostPort + ")"));
            if (clientConfig.isSaslClientEnabled()) {
                try {
                    if (zooKeeperSaslClient != null) {
                        zooKeeperSaslClient.shutdown();
                    }
                    zooKeeperSaslClient = new ZooKeeperSaslClient(SaslServerPrincipal.getServerPrincipal(addr, clientConfig),
                        clientConfig);
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn("SASL configuration failed: " + e + " Will continue connection to Zookeeper server without "
                      + "SASL authentication, if Zookeeper server allows it.");
                    eventThread.queueEvent(new WatchedEvent(
                      Watcher.Event.EventType.None,
                      Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            logStartConnect(addr);
            //nio连接服务器，注册处理器
            clientCnxnSocket.connect(addr);
        }

        private void logStartConnect(InetSocketAddress addr) {
            String msg = "Opening socket connection to server " + addr;
            if (zooKeeperSaslClient != null) {
              msg += ". " + zooKeeperSaslClient.getConfigStatus();
            }
            LOG.info(msg);
        }

        private static final String RETRY_CONN_MSG =
            ", closing socket connection and attempting reconnect";
        @Override
        public void run() {
            clientCnxnSocket.introduce(this, sessionId, outgoingQueue);
            clientCnxnSocket.updateNow();
            //更新最后发送和心跳时间
            clientCnxnSocket.updateLastSendAndHeard();
            int to;
            long lastPingRwServer = Time.currentElapsedTime();
            //ping间隔时间
            final int MAX_SEND_PING_INTERVAL = 10000; //10 seconds
            InetSocketAddress serverAddress = null;
            while (state.isAlive()) {
                //连接有效时的无限循环
                try {
                    if (!clientCnxnSocket.isConnected()) {
                        //未连接，开始建立连接
                        // don't re-establish connection if we are closing
                        if (closing) {
                            break;
                        }
                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            //选一个随机的服务器
                            serverAddress = hostProvider.next(1000);
                        }
                        //只连接1台服务器，这里也是请求和响应顺序性的一个保障
                        startConnect(serverAddress);
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    if (state.isConnected()) {
                        //已连接
                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                   LOG.error("SASL authentication with Zookeeper Quorum member failed: " + e);
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent) {
                                eventThread.queueEvent(new WatchedEvent(
                                      Watcher.Event.EventType.None,
                                      authState,null));
                                if (state == States.AUTH_FAILED) {
                                  eventThread.queueEventOfDeath();
                                }
                            }
                        }
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {
                        //连接未成功
                        //配置的连接超时时长 - 距离最后一次发送心跳包过去了多久
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }
                    
                    if (to <= 0) {
                        //超时
                        String warnInfo;
                        warnInfo = "Client session timed out, have not heard from server in "
                            + clientCnxnSocket.getIdleRecv()
                            + "ms"
                            + " for sessionid 0x"
                            + Long.toHexString(sessionId);
                        LOG.warn(warnInfo);
                        throw new SessionTimeoutException(warnInfo);
                    }
                    if (state.isConnected()) {
                    	//1000(1 second) is to prevent race condition missing to send the second ping
                    	//also make sure not to send too many pings when readTimeout is small 
                        int timeToNextPing = readTimeout / 2 - clientCnxnSocket.getIdleSend() - 
                        		((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        //send a ping request either time is due or no packet sent out within MAX_SEND_PING_INTERVAL
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            sendPing();
                            clientCnxnSocket.updateLastSend();
                        } else {
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    // If we are in read-only mode, seek for read/write server
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout =
                                Math.min(2*pingRwTimeout, maxPingRwTimeout);
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }

                    //和服务端通信传输
                    clientCnxnSocket.doTransport(to, pendingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    if (closing) {
                        if (LOG.isDebugEnabled()) {
                            // closing so this is expected
                            LOG.debug("An exception was thrown while closing send thread for session 0x"
                                    + Long.toHexString(getSessionId())
                                    + " : " + e.getMessage());
                        }
                        break;
                    } else {
                        // this is ugly, you have a better way speak up
                        if (e instanceof SessionExpiredException) {
                            LOG.info(e.getMessage() + ", closing socket connection");
                        } else if (e instanceof SessionTimeoutException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof EndOfStreamException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof RWServerFoundException) {
                            LOG.info(e.getMessage());
                        } else if (e instanceof SocketException) {
                            LOG.info("Socket error occurred: {}: {}", serverAddress, e.getMessage());
                        } else {
                            LOG.warn("Session 0x{} for server {}, unexpected error{}",
                                            Long.toHexString(getSessionId()),
                                            serverAddress,
                                            RETRY_CONN_MSG,
                                            e);
                        }
                        // At this point, there might still be new packets appended to outgoingQueue.
                        // they will be handled in next connection or cleared up if closed.
                        cleanAndNotifyState();
                    }
                }
            }
            synchronized (state) {
                // When it comes to this point, it guarantees that later queued
                // packet to outgoingQueue will be notified of death.
                cleanup();
            }
            clientCnxnSocket.close();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None,
                        Event.KeeperState.Disconnected, null));
            }
            eventThread.queueEvent(new WatchedEvent(Event.EventType.None,
                        Event.KeeperState.Closed, null));
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                    "SendThread exited loop for session: 0x"
                           + Long.toHexString(getSessionId()));
        }

        private void cleanAndNotifyState() {
            cleanup();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None,
                        Event.KeeperState.Disconnected, null));
            }
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
        }

        private void pingRwServer() throws RWServerFoundException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);
            LOG.info("Checking server " + addr + " for being r/w." +
                    " Timeout " + pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostString(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();
                br = new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server " +
                        e.getMessage(), e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }

            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // save the found address so that it's used during the next
                // connection attempt
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at "
                        + addr.getHostString() + ":" + addr.getPort());
            }
        }

        private void cleanup() {
            clientCnxnSocket.cleanup();
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            // We can't call outgoingQueue.clear() here because
            // between iterating and clear up there might be new
            // packets added in queuePacket().
            Iterator<Packet> iter = outgoingQueue.iterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                conLossPacket(p);
                iter.remove();
            }
        }

        /**
         * Callback invoked by the ClientCnxnSocket once a connection has been
         * established.
         *
         * 建立连接后，由ClientCnxnSocket调用的回调。
         * 
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        void onConnected(int _negotiatedSessionTimeout, long _sessionId,
                byte[] _sessionPasswd, boolean isRO) throws IOException {
            negotiatedSessionTimeout = _negotiatedSessionTimeout;
            if (negotiatedSessionTimeout <= 0) {
                //客户端会话超时、过期，标记zk状态为已关闭
                state = States.CLOSED;

                //入队会话过期事件
                eventThread.queueEvent(new WatchedEvent(
                        Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.Expired, null));

                //入队毒丸
                eventThread.queueEventOfDeath();

                String warnInfo;
                warnInfo = "Unable to reconnect to ZooKeeper service, session 0x"
                    + Long.toHexString(sessionId) + " has expired";
                LOG.warn(warnInfo);
                //抛会话过期异常
                throw new SessionExpiredException(warnInfo);
            }
            if (!readOnly && isRO) {
                //配置的客户端不允许进入readOnly模式，但是连接到的服务器发生了分区，服务器进入了readOnly模式
                LOG.error("Read/write client got connected to read-only server");
            }
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();
            //触发成功连接事件，hostProvider实现会标记成功的服务器list下标等
            hostProvider.onConnected();
            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;
            //服务端readOnly的话，标记zk状态为只读，否则为正常的已连接。
            state = (isRO) ?
                    States.CONNECTEDREADONLY : States.CONNECTED;
            //是否连接到过r/w服务器
            seenRwServerBefore |= !isRO;
            LOG.info("Session establishment complete on server "
                    + clientCnxnSocket.getRemoteSocketAddress()
                    + ", sessionid = 0x" + Long.toHexString(sessionId)
                    + ", negotiated timeout = " + negotiatedSessionTimeout
                    + (isRO ? " (READ-ONLY mode)" : ""));
            //watcher判断zk的状态
            KeeperState eventState = (isRO) ?
                    KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            //事件处理线程会入队事件和监听该事件的观察者们，后面会被eventThread执行事件处理逻辑
            eventThread.queueEvent(new WatchedEvent(
                    Watcher.Event.EventType.None,
                    eventState, null));
        }

        void close() {
            state = States.CLOSED;
            clientCnxnSocket.onClosing();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        public boolean tunnelAuthInProgress() {
            // 1. SASL client is disabled.
            if (!clientConfig.isSaslClientEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disconnecting client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        sendThread.close();
        try {
            sendThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted while waiting for the sender thread to close", ex);
        }
        eventThread.queueEventOfDeath();
        if (zooKeeperSaslClient != null) {
            zooKeeperSaslClient.shutdown();
        }
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing client for session: 0x"
                      + Long.toHexString(getSessionId()));
        }

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    // @VisibleForTesting
    protected int xid = 1;

    // @VisibleForTesting
    volatile States state = States.NOT_CONNECTED;

    /*
     * getXid() is called externally by ClientCnxnNIO::doIO() when packets are sent from the outgoingQueue to
     * the server. Thus, getXid() must be public.
     */
    synchronized public int getXid() {
        // Avoid negative cxid values.  In particular, cxid values of -4, -2, and -1 are special and
        // must not be used for requests -- see SendThread.readResponse.
        // Skip from MAX to 1.
        if (xid == Integer.MAX_VALUE) {
            xid = 1;
        }
        return xid++;
    }

    public ReplyHeader submitRequest(RequestHeader h, Record request,
            Record response, WatchRegistration watchRegistration)
            throws InterruptedException {
        return submitRequest(h, request, response, watchRegistration, null);
    }

    public ReplyHeader submitRequest(RequestHeader h, Record request,
            Record response, WatchRegistration watchRegistration,
            WatchDeregistration watchDeregistration)
            throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        Packet packet = queuePacket(h, r, request, response, null, null, null,
                null, watchRegistration, watchDeregistration);
        synchronized (packet) {
            if (requestTimeout > 0) {
                // Wait for request completion with timeout
                waitForPacketFinish(r, packet);
            } else {
                // Wait for request completion infinitely
                while (!packet.finished) {
                    packet.wait();
                }
            }
        }
        if (r.getErr() == Code.REQUESTTIMEOUT.intValue()) {
            sendThread.cleanAndNotifyState();
        }
        return r;
    }

    /**
     * Wait for request completion with timeout.
     */
    private void waitForPacketFinish(ReplyHeader r, Packet packet)
            throws InterruptedException {
        long waitStartTime = Time.currentElapsedTime();
        while (!packet.finished) {
            packet.wait(requestTimeout);
            if (!packet.finished && ((Time.currentElapsedTime()
                    - waitStartTime) >= requestTimeout)) {
                LOG.error("Timeout error occurred for the packet '{}'.",
                        packet);
                r.setErr(Code.REQUESTTIMEOUT.intValue());
                break;
            }
        }
    }

    public void saslCompleted() {
        sendThread.getClientCnxnSocket().saslCompleted();
    }

    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode)
    throws IOException {
        // Generate Xid now because it will be sent immediately,
        // by call to sendThread.sendPacket() below.
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    public Packet queuePacket(RequestHeader h, ReplyHeader r, Record request,
            Record response, AsyncCallback cb, String clientPath,
            String serverPath, Object ctx, WatchRegistration watchRegistration) {
        return queuePacket(h, r, request, response, cb, clientPath, serverPath,
                ctx, watchRegistration, null);
    }

    public Packet queuePacket(RequestHeader h, ReplyHeader r, Record request,
            Record response, AsyncCallback cb, String clientPath,
            String serverPath, Object ctx, WatchRegistration watchRegistration,
            WatchDeregistration watchDeregistration) {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet. It is
        // generated later at send-time, by an implementation of ClientCnxnSocket::doIO(),
        // where the packet is actually sent.
        packet = new Packet(h, r, request, response, watchRegistration);
        packet.cb = cb;
        packet.ctx = ctx;
        packet.clientPath = clientPath;
        packet.serverPath = serverPath;
        packet.watchDeregistration = watchDeregistration;
        // The synchronized block here is for two purpose:
        // 1. synchronize with the final cleanup() in SendThread.run() to avoid race
        // 2. synchronized against each packet. So if a closeSession packet is added,
        // later packet will be notified.
        synchronized (state) {
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then
                // mark as closing
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                outgoingQueue.add(packet);
            }
        }
        sendThread.getClientCnxnSocket().packetAdded();
        return packet;
    }

    public void addAuthInfo(String scheme, byte auth[]) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(new RequestHeader(-4, OpCode.auth), null,
                new AuthPacket(0, scheme, auth), null, null, null, null,
                null, null);
    }

    States getState() {
        return state;
    }

    private static class LocalCallback {
        private final AsyncCallback cb;
        private final int rc;
        private final String path;
        private final Object ctx;

        public LocalCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            this.cb = cb;
            this.rc = rc;
            this.path = path;
            this.ctx = ctx;
        }
    }

    private void initRequestTimeout() {
        try {
            requestTimeout = clientConfig.getLong(
                    ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                    ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT_DEFAULT);
            LOG.info("{} value is {}. feature enabled=",
                    ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                    requestTimeout, requestTimeout > 0);
        } catch (NumberFormatException e) {
            LOG.error(
                    "Configured value {} for property {} can not be parsed to long.",
                    clientConfig.getProperty(
                            ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT),
                    ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT);
            throw e;
        }
    }
}
