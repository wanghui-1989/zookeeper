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

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class maintains the tree data structure. It doesn't have any networking
 * or client connection code in it so that it can be tested in a stand alone
 * way.
 * <p>
 * The tree maintains two parallel data structures: a hashtable that maps from
 * full paths to DataNodes and a tree of DataNodes. All accesses to a path is
 * through the hashtable. The tree is traversed only when serializing to disk.
 *
 * 此类维护树数据结构。 它没有任何网络或客户端连接代码，因此可以以独立方式进行测试。
 * 该树维护两个并行的数据结构：从完整路径映射到DataNode的哈希表和DataNode的树。
 * 对路径的所有访问都是通过哈希表进行的。 仅在序列化到磁盘时遍历该树。
 *
 * 对内存树的操作都在这里，包括快照序列化，反序列化、添加节点、删除节点、修改节点、批量操作等。
 *
 *
 * DataTree的序列化和还原过程：
 * 由两部分完成：1.事务日志 2.快照
 * 1.序列化过程
 *  1.每一次写请求都会生成一个唯一的事务id即zxid，zxid可比较大小，由两部分组成 自增epoch + 自增int。
 *  2.使用zxid组成文件名，每一次的写事务都会被序列化，落到磁盘上。
 *  3.每当积累了一定数量的事务日志后，都会随机触发快照线程的拍摄快照操作。
 *  4.快照线程也是使用zxid拼接作为文件名，从root根节点开始，序列化整个内存DataTree到快照文件中。
 *  5.拍摄快照逻辑保证了，<=zxid的所有数据都被写到了快照文件中，>zxid的数据可能也写入到了快照文件中。
 *
 * 2.反序列化，还原DataTree过程
 *  1.启动时，单线程根据zxid查找最近的100个大概有效的快照文件，按照距离当前时间由近及远排列。
 *  2.遍历快照集合，加载数据，校验数据，如果数据没问题，就使用这个快照，返回快照zxid。
 *  3.此时假设快照文件的zxid=5，内存DataTree就是zxid=5的快照内容反序列化组成的。
 *  4.再将zxid+1=6，去查找>=6的事务日志文件，按顺序重放replay事务日志文件，补齐数据。
 *
 * 到此就完成了树的序列化和还原。
 *
 */
public class DataTree {
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

    /**
     * This hashtable provides a fast lookup to the datanodes. The tree is the
     * source of truth and is where all the locking occurs
     *
     * 该哈希表可快速查找数据节点。 树是真理的源头，是所有锁定发生的地方。
     *
     * 从这里看，zk没有实现自己的树结构，而是使用ConcurrentHashMap来存储树。
     * 使用的默认构造器，即并发级别为16。我觉得有点小，此处没有提供api更改这个的大小。
     * zk是读多于写的，从这点来讲，使用jdk1.8以上版本性能会更好，因为jdk1.8的实现是红黑树，之前是链表。
     *
     * 因为它不是真正的树，所以在节点层级上，权限ACL控制会有问题。
     * 比如访问权限，路径/p/c  假如设置/p不可访问，/c可以访问。
     * 从文件树来说我们设置了父文件夹不可访问，对应的子文件一定不可以访问，所以上面说的权限设置不能实现。
     * 然而zk的树因为不是真的按树层级遍历路径（或者说树的层级遍历源码由jdk控制，没有提供扩展，外部无法改变），而是map，
     * 所以设置/p不可访问，/c可以访问应该是能实现的，而且/p/c的确可以访问，
     * 因为根本就没有先判断父路径/p的权限，而是直接map定位目标路径，相当于绕过了父路径权限，严格讲可以算是漏洞。
     */
    private final ConcurrentHashMap<String, DataNode> nodes =
        new ConcurrentHashMap<String, DataNode>();

    private final WatchManager dataWatches = new WatchManager();

    private final WatchManager childWatches = new WatchManager();

    /** the root of zookeeper tree */
    private static final String rootZookeeper = "/";

    /** the zookeeper nodes that acts as the management and status node **/
    private static final String procZookeeper = Quotas.procZookeeper;

    /** this will be the string thats stored as a child of root */
    private static final String procChildZookeeper = procZookeeper.substring(1);

    /**
     * the zookeeper quota node that acts as the quota management node for
     * zookeeper
     */
    private static final String quotaZookeeper = Quotas.quotaZookeeper;

    /** this will be the string thats stored as a child of /zookeeper */
    private static final String quotaChildZookeeper = quotaZookeeper
            .substring(procZookeeper.length() + 1);

    /**
     * the zookeeper config node that acts as the config management node for
     * zookeeper
     */
    private static final String configZookeeper = ZooDefs.CONFIG_NODE;

    /** this will be the string thats stored as a child of /zookeeper */
    private static final String configChildZookeeper = configZookeeper
            .substring(procZookeeper.length() + 1);

    /**
     * the path trie that keeps track fo the quota nodes in this datatree
     * 路径前缀树(字典树)
     */
    private final PathTrie pTrie = new PathTrie();

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     * session和对应的路径集合。存储某个session和它创建的路径，在session失效后，因为是临时节点，应该删除创建的所有节点。
     */
    private final Map<Long, HashSet<String>> ephemerals =
        new ConcurrentHashMap<Long, HashSet<String>>();

    /**
     * This set contains the paths of all container nodes
     */
    private final Set<String> containers =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * This set contains the paths of all ttl nodes
     */
    private final Set<String> ttls =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final ReferenceCountedACLCache aclCache = new ReferenceCountedACLCache();

    @SuppressWarnings("unchecked")
    public Set<String> getEphemerals(long sessionId) {
        HashSet<String> retv = ephemerals.get(sessionId);
        if (retv == null) {
            return new HashSet<String>();
        }
        HashSet<String> cloned = null;
        synchronized (retv) {
            cloned = (HashSet<String>) retv.clone();
        }
        return cloned;
    }

    public Set<String> getContainers() {
        return new HashSet<String>(containers);
    }

    public Set<String> getTtls() {
        return new HashSet<String>(ttls);
    }

    public Collection<Long> getSessions() {
        return ephemerals.keySet();
    }

    public DataNode getNode(String path) {
        return nodes.get(path);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    public int getEphemeralsCount() {
        int result = 0;
        for (HashSet<String> set : ephemerals.values()) {
            result += set.size();
        }
        return result;
    }

    /**
     * Get the size of the nodes based on path and data length.
     *
     * @return size of the data
     */
    public long approximateDataSize() {
        long result = 0;
        for (Map.Entry<String, DataNode> entry : nodes.entrySet()) {
            DataNode value = entry.getValue();
            synchronized (value) {
                result += entry.getKey().length();
                result += value.getApproximateDataSize();
            }
        }
        return result;
    }

    /**
     * This is a pointer to the root of the DataTree. It is the source of truth,
     * but we usually use the nodes hashmap to find nodes in the tree.
     *
     * DataTree根节点
     */
    private DataNode root = new DataNode(new byte[0], -1L, new StatPersisted());

    /**
     * create a /zookeeper filesystem that is the proc filesystem of zookeeper
     */
    private final DataNode procDataNode = new DataNode(new byte[0], -1L, new StatPersisted());

    /**
     * create a /zookeeper/quota node for maintaining quota properties for
     * zookeeper
     */
    private final DataNode quotaDataNode = new DataNode(new byte[0], -1L, new StatPersisted());

    public DataTree() {
        /* Rather than fight it, let root have an alias */
        //空串和/都对应根节点
        nodes.put("", root);
        nodes.put(rootZookeeper, root);

        /** add the proc node and quota node */
        root.addChild(procChildZookeeper);
        nodes.put(procZookeeper, procDataNode);

        procDataNode.addChild(quotaChildZookeeper);
        nodes.put(quotaZookeeper, quotaDataNode);

        addConfigNode();
    }

    /**
     * create a /zookeeper/config node for maintaining the configuration (membership and quorum system) info for
     * zookeeper
     */
    public void addConfigNode() {
        DataNode zookeeperZnode = nodes.get(procZookeeper);
        if (zookeeperZnode != null) { // should always be the case
            zookeeperZnode.addChild(configChildZookeeper);
        } else {
            assert false : "There's no /zookeeper znode - this should never happen.";
        }

        nodes.put(configZookeeper, new DataNode(new byte[0], -1L, new StatPersisted()));
        try {
            // Reconfig node is access controlled by default (ZOOKEEPER-2014).
            setACL(configZookeeper, ZooDefs.Ids.READ_ACL_UNSAFE, -1);
        } catch (KeeperException.NoNodeException e) {
            assert false : "There's no " + configZookeeper +
                    " znode - this should never happen.";
        }
    }

    /**
     * is the path one of the special paths owned by zookeeper.
     *
     * @param path
     *            the path to be checked
     * @return true if a special path. false if not.
     */
    boolean isSpecialPath(String path) {
        if (rootZookeeper.equals(path) || procZookeeper.equals(path)
                || quotaZookeeper.equals(path) || configZookeeper.equals(path)) {
            return true;
        }
        return false;
    }

    static public void copyStatPersisted(StatPersisted from, StatPersisted to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
    }

    static public void copyStat(Stat from, Stat to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
        to.setDataLength(from.getDataLength());
        to.setNumChildren(from.getNumChildren());
    }

    /**
     * update the count of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed.
     * @param diff
     *            the diff to be added to the count
     */
    public void updateCount(String lastPrefix, int diff) {
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        StatsTrack updatedStat = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for stat " + statNode);
            return;
        }
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setCount(updatedStat.getCount() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the counts match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        StatsTrack thisStats = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for quota " + quotaNode);
            return;
        }
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getCount() > -1 && (thisStats.getCount() < updatedStat.getCount())) {
            LOG
            .warn("Quota exceeded: " + lastPrefix + " count="
                    + updatedStat.getCount() + " limit="
                    + thisStats.getCount());
        }
    }

    /**
     * update the count of bytes of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed
     * @param diff
     *            the diff to added to number of bytes
     * @throws IOException
     *             if path is not found
     */
    public void updateBytes(String lastPrefix, long diff) {
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing stat node for bytes " + statNode);
            return;
        }
        StatsTrack updatedStat = null;
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setBytes(updatedStat.getBytes() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the bytes match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing quota node for bytes " + quotaNode);
            return;
        }
        StatsTrack thisStats = null;
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) {
            LOG
            .warn("Quota exceeded: " + lastPrefix + " bytes="
                    + updatedStat.getBytes() + " limit="
                    + thisStats.getBytes());
        }
    }

    /**
     * Add a new node to the DataTree.
     * @param path
     * 			  Path for the new node.
     * @param data
     *            Data to store in the node.
     * @param acl
     *            Node acls
     * @param ephemeralOwner
     *            the session id that owns this node. -1 indicates this is not
     *            an ephemeral node.
     * @param zxid
     *            Transaction ID
     * @param time
     * @throws NodeExistsException
     * @throws NoNodeException
     * @throws KeeperException
     */
    public void createNode(final String path, byte data[], List<ACL> acl,
            long ephemeralOwner, int parentCVersion, long zxid, long time)
    		throws NoNodeException, NodeExistsException {
    	createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, null);
    }

    /**
     * Add a new node to the DataTree.
     * 更新内存树了
     * @param path
     * 			  Path for the new node.
     * @param data
     *            Data to store in the node.
     * @param acl
     *            Node acls
     * @param ephemeralOwner
     *            the session id that owns this node. -1 indicates this is not
     *            an ephemeral node.
     * @param zxid
     *            Transaction ID
     * @param time
     * @param outputStat
     * 			  A Stat object to store Stat output results into.
     * @throws NodeExistsException
     * @throws NoNodeException
     * @throws KeeperException
     */
    public void createNode(final String path, byte data[], List<ACL> acl,
            long ephemeralOwner, int parentCVersion, long zxid, long time, Stat outputStat)
            throws KeeperException.NoNodeException,
            KeeperException.NodeExistsException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash);
        String childName = path.substring(lastSlash + 1);
        StatPersisted stat = new StatPersisted();
        stat.setCtime(time);
        stat.setMtime(time);
        stat.setCzxid(zxid);
        stat.setMzxid(zxid);
        stat.setPzxid(zxid);
        stat.setVersion(0);
        stat.setAversion(0);
        stat.setEphemeralOwner(ephemeralOwner);
        DataNode parent = nodes.get(parentName);
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            Set<String> children = parent.getChildren();
            if (children.contains(childName)) {
                throw new KeeperException.NodeExistsException();
            }

            if (parentCVersion == -1) {
                parentCVersion = parent.stat.getCversion();
                parentCVersion++;
            }
            parent.stat.setCversion(parentCVersion);
            parent.stat.setPzxid(zxid);
            Long longval = aclCache.convertAcls(acl);
            //创建新节点
            DataNode child = new DataNode(data, longval, stat);
            //加到parent.children中
            parent.addChild(childName);
            //放到树中，即ConcurrentHashMap中
            nodes.put(path, child);
            EphemeralType ephemeralType = EphemeralType.get(ephemeralOwner);
            if (ephemeralType == EphemeralType.CONTAINER) {
                containers.add(path);
            } else if (ephemeralType == EphemeralType.TTL) {
                ttls.add(path);
            } else if (ephemeralOwner != 0) {
                //临时节点 获取该session对应的所有临时节点路径集合
                HashSet<String> list = ephemerals.get(ephemeralOwner);
                if (list == null) {
                    list = new HashSet<String>();
                    ephemerals.put(ephemeralOwner, list);
                }
                synchronized (list) {
                    list.add(path);
                }
            }
            if (outputStat != null) {
            	child.copyStat(outputStat);
            }
        }
        // now check if its one of the zookeeper node child
        if (parentName.startsWith(quotaZookeeper)) {
            // now check if its the limit node
            if (Quotas.limitNode.equals(childName)) {
                // this is the limit node
                // get the parent and add it to the trie
                pTrie.addPath(parentName.substring(quotaZookeeper.length()));
            }
            if (Quotas.statNode.equals(childName)) {
                updateQuotaForPath(parentName
                        .substring(quotaZookeeper.length()));
            }
        }
        // also check to update the quotas for this node
        String lastPrefix = getMaxPrefixWithQuota(path);
        if(lastPrefix != null) {
            // ok we have some match and need to update
            updateCount(lastPrefix, 1);
            updateBytes(lastPrefix, data == null ? 0 : data.length);
        }
        //触发事件通知
        dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                Event.EventType.NodeChildrenChanged);
    }

    /**
     * remove the path from the datatree
     *
     * @param path
     *            the path to of the node to be deleted
     * @param zxid
     *            the current zxid
     * @throws KeeperException.NoNodeException
     */
    public void deleteNode(String path, long zxid)
            throws KeeperException.NoNodeException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash);
        String childName = path.substring(lastSlash + 1);

        // The child might already be deleted during taking fuzzy snapshot,
        // but we still need to update the pzxid here before throw exception
        // for no such child
        DataNode parent = nodes.get(parentName);
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            parent.removeChild(childName);
            // Only update pzxid when the zxid is larger than the current pzxid,
            // otherwise we might override higher pzxid set by a following create 
            // Txn, which could cause the cversion and pzxid inconsistent
            if (zxid > parent.stat.getPzxid()) {
                parent.stat.setPzxid(zxid);
            }
        }

        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException();
        }
        nodes.remove(path);
        synchronized (node) {
            aclCache.removeUsage(node.acl);
        }

        // Synchronized to sync the containers and ttls change, probably
        // only need to sync on containers and ttls, will update it in a
        // separate patch.
        synchronized (parent) {
            long eowner = node.stat.getEphemeralOwner();
            EphemeralType ephemeralType = EphemeralType.get(eowner);
            if (ephemeralType == EphemeralType.CONTAINER) {
                containers.remove(path);
            } else if (ephemeralType == EphemeralType.TTL) {
                ttls.remove(path);
            } else if (eowner != 0) {
                HashSet<String> nodes = ephemerals.get(eowner);
                if (nodes != null) {
                    synchronized (nodes) {
                        nodes.remove(path);
                    }
                }
            }
        }

        if (parentName.startsWith(procZookeeper) && Quotas.limitNode.equals(childName)) {
            // delete the node in the trie.
            // we need to update the trie as well
            pTrie.deletePath(parentName.substring(quotaZookeeper.length()));
        }

        // also check to update the quotas for this node
        String lastPrefix = getMaxPrefixWithQuota(path);
        if(lastPrefix != null) {
            // ok we have some match and need to update
            updateCount(lastPrefix, -1);
            int bytes = 0;
            synchronized (node) {
                bytes = (node.data == null ? 0 : -(node.data.length));
            }
            updateBytes(lastPrefix, bytes);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "dataWatches.triggerWatch " + path);
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "childWatches.triggerWatch " + parentName);
        }
        Set<Watcher> processed = dataWatches.triggerWatch(path,
                EventType.NodeDeleted);
        childWatches.triggerWatch(path, EventType.NodeDeleted, processed);
        childWatches.triggerWatch("".equals(parentName) ? "/" : parentName,
                EventType.NodeChildrenChanged);
    }

    public Stat setData(String path, byte data[], int version, long zxid,
            long time) throws KeeperException.NoNodeException {
        Stat s = new Stat();
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        byte lastdata[] = null;
        //使用要修改的节点作为锁
        synchronized (n) {
            lastdata = n.data;
            n.data = data;
            n.stat.setMtime(time);
            n.stat.setMzxid(zxid);
            n.stat.setVersion(version);
            n.copyStat(s);
        }
        // now update if the path is in a quota subtree.
        String lastPrefix = getMaxPrefixWithQuota(path);
        if(lastPrefix != null) {
          this.updateBytes(lastPrefix, (data == null ? 0 : data.length)
              - (lastdata == null ? 0 : lastdata.length));
        }
        dataWatches.triggerWatch(path, EventType.NodeDataChanged);
        return s;
    }

    /**
     * If there is a quota set, return the appropriate prefix for that quota
     * Else return null
     * @param path The ZK path to check for quota
     * @return Max quota prefix, or null if none
     */
    public String getMaxPrefixWithQuota(String path) {
        // do nothing for the root.
        // we are not keeping a quota on the zookeeper
        // root node for now.
        String lastPrefix = pTrie.findMaxPrefix(path);

        if (rootZookeeper.equals(lastPrefix) || "".equals(lastPrefix)) {
            return null;
        }
        else {
            return lastPrefix;
        }
    }

    public byte[] getData(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            if (watcher != null) {
                dataWatches.addWatch(path, watcher);
            }
            return n.data;
        }
    }

    public Stat statNode(String path, Watcher watcher)
            throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (watcher != null) {
            dataWatches.addWatch(path, watcher);
        }
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return stat;
        }
    }

    public List<String> getChildren(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            if (stat != null) {
                n.copyStat(stat);
            }
            List<String> children=new ArrayList<String>(n.getChildren());

            if (watcher != null) {
                childWatches.addWatch(path, watcher);
            }
            return children;
        }
    }

    public Stat setACL(String path, List<ACL> acl, int version)
            throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            aclCache.removeUsage(n.acl);
            n.stat.setAversion(version);
            n.acl = aclCache.convertAcls(acl);
            n.copyStat(stat);
            return stat;
        }
    }

    public List<ACL> getACL(String path, Stat stat)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return new ArrayList<ACL>(aclCache.convertLong(n.acl));
        }
    }

    public List<ACL> getACL(DataNode node) {
        synchronized (node) {
            return aclCache.convertLong(node.acl);
        }
    }

    public int aclCacheSize() {
        return aclCache.size();
    }

    static public class ProcessTxnResult {
        public long clientId;

        public int cxid;

        public long zxid;

        public int err;

        public int type;

        public String path;

        public Stat stat;

        public List<ProcessTxnResult> multiResult;

        /**
         * Equality is defined as the clientId and the cxid being the same. This
         * allows us to use hash tables to track completion of transactions.
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (o instanceof ProcessTxnResult) {
                ProcessTxnResult other = (ProcessTxnResult) o;
                return other.clientId == clientId && other.cxid == cxid;
            }
            return false;
        }

        /**
         * See equals() to find the rational for how this hashcode is generated.
         *
         * @see ProcessTxnResult#equals(Object)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return (int) ((clientId ^ cxid) % Integer.MAX_VALUE);
        }

    }

    public volatile long lastProcessedZxid = 0;

    public ProcessTxnResult processTxn(TxnHeader header, Record txn) {
        return this.processTxn(header, txn, false);
    }

    /**
     * 根据请求类型，对内存树执行创建节点、删除节点、修改节点、权限以及批量执行等操作，返回操作结果。
     */
    public ProcessTxnResult processTxn(TxnHeader header, Record txn, boolean isSubTxn)
    {
        ProcessTxnResult rc = new ProcessTxnResult();

        try {
            rc.clientId = header.getClientId();
            rc.cxid = header.getCxid();
            rc.zxid = header.getZxid();
            rc.type = header.getType();
            rc.err = 0;
            rc.multiResult = null;
            switch (header.getType()) {
                case OpCode.create:
                    CreateTxn createTxn = (CreateTxn) txn;
                    rc.path = createTxn.getPath();
                    createNode(
                            createTxn.getPath(),
                            createTxn.getData(),
                            createTxn.getAcl(),
                            createTxn.getEphemeral() ? header.getClientId() : 0,
                            createTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), null);
                    break;
                case OpCode.create2:
                    CreateTxn create2Txn = (CreateTxn) txn;
                    rc.path = create2Txn.getPath();
                    Stat stat = new Stat();
                    createNode(
                            create2Txn.getPath(),
                            create2Txn.getData(),
                            create2Txn.getAcl(),
                            create2Txn.getEphemeral() ? header.getClientId() : 0,
                            create2Txn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.createTTL:
                    CreateTTLTxn createTtlTxn = (CreateTTLTxn) txn;
                    rc.path = createTtlTxn.getPath();
                    stat = new Stat();
                    createNode(
                            createTtlTxn.getPath(),
                            createTtlTxn.getData(),
                            createTtlTxn.getAcl(),
                            EphemeralType.TTL.toEphemeralOwner(createTtlTxn.getTtl()),
                            createTtlTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.createContainer:
                    CreateContainerTxn createContainerTxn = (CreateContainerTxn) txn;
                    rc.path = createContainerTxn.getPath();
                    stat = new Stat();
                    createNode(
                            createContainerTxn.getPath(),
                            createContainerTxn.getData(),
                            createContainerTxn.getAcl(),
                            EphemeralType.CONTAINER_EPHEMERAL_OWNER,
                            createContainerTxn.getParentCVersion(),
                            header.getZxid(), header.getTime(), stat);
                    rc.stat = stat;
                    break;
                case OpCode.delete:
                case OpCode.deleteContainer:
                    DeleteTxn deleteTxn = (DeleteTxn) txn;
                    rc.path = deleteTxn.getPath();
                    deleteNode(deleteTxn.getPath(), header.getZxid());
                    break;
                case OpCode.reconfig:
                case OpCode.setData:
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    rc.path = setDataTxn.getPath();
                    rc.stat = setData(setDataTxn.getPath(), setDataTxn
                            .getData(), setDataTxn.getVersion(), header
                            .getZxid(), header.getTime());
                    break;
                case OpCode.setACL:
                    SetACLTxn setACLTxn = (SetACLTxn) txn;
                    rc.path = setACLTxn.getPath();
                    rc.stat = setACL(setACLTxn.getPath(), setACLTxn.getAcl(),
                            setACLTxn.getVersion());
                    break;
                case OpCode.closeSession:
                    killSession(header.getClientId(), header.getZxid());
                    break;
                case OpCode.error:
                    ErrorTxn errTxn = (ErrorTxn) txn;
                    rc.err = errTxn.getErr();
                    break;
                case OpCode.check:
                    CheckVersionTxn checkTxn = (CheckVersionTxn) txn;
                    rc.path = checkTxn.getPath();
                    break;
                case OpCode.multi:
                    //批量操作
                    MultiTxn multiTxn = (MultiTxn) txn ;
                    List<Txn> txns = multiTxn.getTxns();
                    rc.multiResult = new ArrayList<ProcessTxnResult>();
                    boolean failed = false;
                    for (Txn subtxn : txns) {
                        if (subtxn.getType() == OpCode.error) {
                            failed = true;
                            break;
                        }
                    }

                    boolean post_failed = false;
                    for (Txn subtxn : txns) {
                        ByteBuffer bb = ByteBuffer.wrap(subtxn.getData());
                        Record record = null;
                        switch (subtxn.getType()) {
                            case OpCode.create:
                                record = new CreateTxn();
                                break;
                            case OpCode.createTTL:
                                record = new CreateTTLTxn();
                                break;
                            case OpCode.createContainer:
                                record = new CreateContainerTxn();
                                break;
                            case OpCode.delete:
                            case OpCode.deleteContainer:
                                record = new DeleteTxn();
                                break;
                            case OpCode.setData:
                                record = new SetDataTxn();
                                break;
                            case OpCode.error:
                                record = new ErrorTxn();
                                post_failed = true;
                                break;
                            case OpCode.check:
                                record = new CheckVersionTxn();
                                break;
                            default:
                                throw new IOException("Invalid type of op: " + subtxn.getType());
                        }
                        assert(record != null);

                        ByteBufferInputStream.byteBuffer2Record(bb, record);

                        if (failed && subtxn.getType() != OpCode.error){
                            int ec = post_failed ? Code.RUNTIMEINCONSISTENCY.intValue()
                                                 : Code.OK.intValue();

                            subtxn.setType(OpCode.error);
                            record = new ErrorTxn(ec);
                        }

                        if (failed) {
                            assert(subtxn.getType() == OpCode.error) ;
                        }

                        TxnHeader subHdr = new TxnHeader(header.getClientId(), header.getCxid(),
                                                         header.getZxid(), header.getTime(),
                                                         subtxn.getType());
                        ProcessTxnResult subRc = processTxn(subHdr, record, true);
                        rc.multiResult.add(subRc);
                        if (subRc.err != 0 && rc.err == 0) {
                            rc.err = subRc.err ;
                        }
                    }
                    break;
            }
        } catch (KeeperException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
            rc.err = e.code().intValue();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
        }


        /**
         * Things we can only update after the whole txn is applied to data
         * tree.
         *
         * If we update the lastProcessedZxid with the first sub txn in multi
         * and there is a snapshot in progress, it's possible that the zxid
         * associated with the snapshot only include partial of the multi op.
         *
         * When loading snapshot, it will only load the txns after the zxid
         * associated with snapshot file, which could cause data inconsistency
         * due to missing sub txns.
         *
         * To avoid this, we only update the lastProcessedZxid when the whole
         * multi-op txn is applied to DataTree.
         *
         * 翻译：只有将整个txn应用于数据树之后，我们才能更新事务。
         * 如果我们用multi中的第一个子txn更新了lastProcessedZxid，并且有一个正在进行的快照，
         * 则与快照关联的zxid可能仅包括multi op的一部分。加载快照时，它将仅在与快照文件关联的zxid之后加载txns，
         * 这可能会由于缺少子txns而导致数据不一致。为了避免这种情况，
         * 我们仅在将整个多操作txn应用于DataTree时才更新lastProcessedZxid。
         */
        if (!isSubTxn) {
            /**
             * A snapshot might be in progress while we are modifying the data
             * tree. If we set lastProcessedZxid prior to making corresponding
             * change to the tree, then the zxid associated with the snapshot
             * file will be ahead of its contents. Thus, while restoring from
             * the snapshot, the restore method will not apply the transaction
             * for zxid associated with the snapshot file, since the restore
             * method assumes that transaction to be present in the snapshot.
             *
             * To avoid this, we first apply the transaction and then modify
             * lastProcessedZxid.  During restore, we correctly handle the
             * case where the snapshot contains data ahead of the zxid associated
             * with the file.
             *
             * 翻译：在我们修改数据树时，可能正在快照。如果在对树进行相应更改之前设置了lastProcessedZxid，
             * 则与快照文件关联的zxid将位于其内容之前。因此，在从快照还原时，还原方法将不会将事务应用于与快照文件关联的zxid，
             * 因为还原方法假定该事务存在于快照中。为了避免这种情况，我们首先应用事务，然后修改lastProcessedZxid。
             * 在还原过程中，我们可以正确处理快照包含与文件关联的zxid之前的数据的情况。
             *
             * 个人理解：假如先将lastProcessedZxid修改为本次zxid，然后在继续执行本次事务之前，快照开始拍摄，
             * 快照取的lastProcessedZxid为本次的zxid，快照序列化完成之后，本次事务才开始执行添加节点、修改节点操作，
             * 这样在快照中就相当于没有将zxid和对应的事务序列化保存。在还原的时候，根据zxid进行还原，此时快照文件中没有本次事务，
             * 后面会根据zxid比对，将大于zxid的事务日志应用到DataTree，小于zxid的事务日志不会处理，因为程序认为快照文件应该包含了本次的事务，
             * 这样本次事务不会被还原，就丢失了。所以先添加到DataTree，然后再更新lastProcessedZxid。
             */
            if (rc.zxid > lastProcessedZxid) {
                lastProcessedZxid = rc.zxid;
            }
        }

        /**
         * Snapshots are taken lazily. It can happen that the child
         * znodes of a parent are created after the parent
         * is serialized. Therefore, while replaying logs during restore, a
         * create might fail because the node was already
         * created.
         *
         * After seeing this failure, we should increment
         * the cversion of the parent znode since the parent was serialized
         * before its children.
         *
         * Note, such failures on DT should be seen only during
         * restore.
         *
         * 在快照树的基础上，还原事务日志，假如快照zxid=5，在拍快照的时候，并发修改了树，假设这个操作为创建节点/a/b，zxid=6。
         * 在快照线程执行/a节点序列化之前，创建节点线程先操作完了，此时快照里就会包括了新的节点/a/b。
         *
         * 在还原快照文件后，DataTree上已经有了/a/b节点，此时执行zxid=6的事务日志，再次创建/a/b节点，
         * 会抛出KeeperException.NodeExistsException，上面捕获后，将rc.err = exception.code().intValue();
         * 到这里要判断是否是这个节点已存在异常，如果是的话修改下节点的指标值，同时也避免这个异常向上抛出影响了树的还原，
         * 因为这个不是什么问题。
         *
         */
        if (header.getType() == OpCode.create &&
                rc.err == Code.NODEEXISTS.intValue()) {
            LOG.debug("Adjusting parent cversion for Txn: " + header.getType() +
                    " path:" + rc.path + " err: " + rc.err);
            int lastSlash = rc.path.lastIndexOf('/');
            String parentName = rc.path.substring(0, lastSlash);
            CreateTxn cTxn = (CreateTxn)txn;
            try {
                setCversionPzxid(parentName, cTxn.getParentCVersion(),
                        header.getZxid());
            } catch (KeeperException.NoNodeException e) {
                LOG.error("Failed to set parent cversion for: " +
                      parentName, e);
                rc.err = e.code().intValue();
            }
        } else if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr: " + header.getType() +
                  " : error: " + rc.err);
        }
        return rc;
    }

    void killSession(long session, long zxid) {
        // the list is already removed from the ephemerals
        // so we do not have to worry about synchronizing on
        // the list. This is only called from FinalRequestProcessor
        // so there is no need for synchronization. The list is not
        // changed here. Only create and delete change the list which
        // are again called from FinalRequestProcessor in sequence.
        HashSet<String> list = ephemerals.remove(session);
        if (list != null) {
            for (String path : list) {
                try {
                    deleteNode(path, zxid);
                    if (LOG.isDebugEnabled()) {
                        LOG
                                .debug("Deleting ephemeral node " + path
                                        + " for session 0x"
                                        + Long.toHexString(session));
                    }
                } catch (NoNodeException e) {
                    LOG.warn("Ignoring NoNodeException for path " + path
                            + " while removing ephemeral for dead session 0x"
                            + Long.toHexString(session));
                }
            }
        }
    }

    /**
     * a encapsultaing class for return value
     */
    private static class Counts {
        long bytes;
        int count;
    }

    /**
     * this method gets the count of nodes and the bytes under a subtree
     *
     * @param path
     *            the path to be used
     * @param counts
     *            the int count
     */
    private void getCounts(String path, Counts counts) {
        DataNode node = getNode(path);
        if (node == null) {
            return;
        }
        String[] children = null;
        int len = 0;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            children = childs.toArray(new String[childs.size()]);
            len = (node.data == null ? 0 : node.data.length);
        }
        // add itself
        counts.count += 1;
        counts.bytes += len;
        for (String child : children) {
            getCounts(path + "/" + child, counts);
        }
    }

    /**
     * update the quota for the given path
     *
     * @param path
     *            the path to be used
     */
    private void updateQuotaForPath(String path) {
        Counts c = new Counts();
        getCounts(path, c);
        StatsTrack strack = new StatsTrack();
        strack.setBytes(c.bytes);
        strack.setCount(c.count);
        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode;
        DataNode node = getNode(statPath);
        // it should exist
        if (node == null) {
            LOG.warn("Missing quota stat node " + statPath);
            return;
        }
        synchronized (node) {
            node.data = strack.toString().getBytes();
        }
    }

    /**
     * this method traverses the quota path and update the path trie and sets
     *
     * @param path
     */
    private void traverseNode(String path) {
        DataNode node = getNode(path);
        String children[] = null;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            children = childs.toArray(new String[childs.size()]);
        }
        if (children.length == 0) {
            // this node does not have a child
            // is the leaf node
            // check if its the leaf node
            String endString = "/" + Quotas.limitNode;
            if (path.endsWith(endString)) {
                // ok this is the limit node
                // get the real node and update
                // the count and the bytes
                String realPath = path.substring(Quotas.quotaZookeeper
                        .length(), path.indexOf(endString));
                updateQuotaForPath(realPath);
                this.pTrie.addPath(realPath);
            }
            return;
        }
        for (String child : children) {
            traverseNode(path + "/" + child);
        }
    }

    /**
     * this method sets up the path trie and sets up stats for quota nodes
     */
    private void setupQuota() {
        String quotaPath = Quotas.quotaZookeeper;
        DataNode node = getNode(quotaPath);
        if (node == null) {
            return;
        }
        traverseNode(quotaPath);
    }

    /**
     * this method uses a stringbuilder to create a new path for children. This
     * is faster than string appends ( str1 + str2).
     *
     * 这里是zk快照核心逻辑：
     *  从DataTree的根节点开始，使用前序遍历的方式，访问每一个节点，序列化节点的路径和对应数据到磁盘。
     *  相当于使用前序遍历的方式，将内存中的整个树序列化到磁盘。
     *
     *
     * @param oa
     *            OutputArchive to write to.
     * @param path
     *            a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        //开始拍摄快照时，第一次传入的path为""
        //DataTree在构造的时候，空字符串和/都对应根节点
        String pathString = path.toString();
        //每次都是取要序列化的节点，第一次取出来的是根节点
        DataNode node = getNode(pathString);
        if (node == null) {
            return;
        }
        String children[] = null;
        DataNode nodeCopy;
        //节点锁，使用要序列化的节点作为锁
        //假如在快照线程对当前节点序列化的同时，有一个线程调用setData修改节点内容，怎么处理的？
        //查看修改内存树的源码：org.apache.zookeeper.server.DataTree.setData
        //setData方法里面也会先从ConcurrentHashMap中取这个节点对象，使用这个节点对象作为锁，
        //锁定以后，再修改节点的内容。就是使用互斥锁来保证只有一个线程可以操作数据。
        //这里在使用快照来恢复数据的时候就不知道怎么处理了，有新有旧
        synchronized (node) {
            StatPersisted statCopy = new StatPersisted();
            copyStatPersisted(node.stat, statCopy);
            //we do not need to make a copy of node.data because the contents
            //are never changed
            nodeCopy = new DataNode(node.data, node.acl, statCopy);
            Set<String> childs = node.getChildren();
            //拿到所有子节点，遍历使用
            children = childs.toArray(new String[childs.size()]);
        }
        //序列化path和对应节点
        serializeNodeData(oa, pathString, nodeCopy);
        path.append('/');
        int off = path.length();
        //遍历所有子节点，序列化
        for (String child : children) {
            // since this is single buffer being resused
            // we need
            // to truncate the previous bytes of string.
            path.delete(off, Integer.MAX_VALUE);
            path.append(child);
            //递归序列化，类似于树的前序遍历
            serializeNode(oa, path);
        }
    }

    // visiable for test
    public void serializeNodeData(OutputArchive oa, String path, DataNode node) throws IOException {
        oa.writeString(path, "path");
        oa.writeRecord(node, "node");
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        //序列化acl
        aclCache.serialize(oa);
        //序列化节点
        serializeNode(oa, new StringBuilder(""));
        // / marks end of stream
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    public void deserialize(InputArchive ia, String tag) throws IOException {
        //反序列化acl
        aclCache.deserialize(ia);
        nodes.clear();
        pTrie.clear();
        String path = ia.readString("path");
        while (!"/".equals(path)) {
            DataNode node = new DataNode();
            ia.readRecord(node, "node");
            nodes.put(path, node);
            synchronized (node) {
                aclCache.addUsage(node.acl);
            }
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1) {
                root = node;
            } else {
                String parentPath = path.substring(0, lastSlash);
                DataNode parent = nodes.get(parentPath);
                if (parent == null) {
                    throw new IOException("Invalid Datatree, unable to find " +
                            "parent " + parentPath + " of path " + path);
                }
                //添加子节点路径名称
                parent.addChild(path.substring(lastSlash + 1));
                long eowner = node.stat.getEphemeralOwner();
                EphemeralType ephemeralType = EphemeralType.get(eowner);
                if (ephemeralType == EphemeralType.CONTAINER) {
                    containers.add(path);
                } else if (ephemeralType == EphemeralType.TTL) {
                    ttls.add(path);
                } else if (eowner != 0) {
                    HashSet<String> list = ephemerals.get(eowner);
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list);
                    }
                    list.add(path);
                }
            }
            path = ia.readString("path");
        }
        nodes.put("/", root);
        // we are done with deserializing the
        // the datatree
        // update the quotas - create path trie
        // and also update the stat nodes
        setupQuota();

        aclCache.purgeUnused();
    }

    /**
     * Summary of the watches on the datatree.
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatchesSummary(PrintWriter pwriter) {
        pwriter.print(dataWatches.toString());
    }

    /**
     * Write a text dump of all the watches on the datatree.
     * Warning, this is expensive, use sparingly!
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        dataWatches.dumpWatches(pwriter, byPath);
    }

    /**
     * Returns a watch report.
     *
     * @return watch report
     * @see WatchesReport
     */
    public synchronized WatchesReport getWatches() {
        return dataWatches.getWatches();
    }

    /**
     * Returns a watch report by path.
     *
     * @return watch report
     * @see WatchesPathReport
     */
    public synchronized WatchesPathReport getWatchesByPath() {
        return dataWatches.getWatchesByPath();
    }

    /**
     * Returns a watch summary.
     *
     * @return watch summary
     * @see WatchesSummary
     */
    public synchronized WatchesSummary getWatchesSummary() {
        return dataWatches.getWatchesSummary();
    }

    /**
     * Write a text dump of all the ephemerals in the datatree.
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        pwriter.println("Sessions with Ephemerals ("
                + ephemerals.keySet().size() + "):");
        for (Entry<Long, HashSet<String>> entry : ephemerals.entrySet()) {
            pwriter.print("0x" + Long.toHexString(entry.getKey()));
            pwriter.println(":");
            HashSet<String> tmp = entry.getValue();
            if (tmp != null) {
                synchronized (tmp) {
                    for (String path : tmp) {
                        pwriter.println("\t" + path);
                    }
                }
            }
        }
    }

    /**
     * Returns a mapping of session ID to ephemeral znodes.
     *
     * @return map of session ID to sets of ephemeral znodes
     */
    public Map<Long, Set<String>> getEphemerals() {
        HashMap<Long, Set<String>> ephemeralsCopy = new HashMap<Long, Set<String>>();
        for (Entry<Long, HashSet<String>> e : ephemerals.entrySet()) {
            synchronized (e.getValue()) {
                ephemeralsCopy.put(e.getKey(), new HashSet<String>(e.getValue()));
            }
        }
        return ephemeralsCopy;
    }

    public void removeCnxn(Watcher watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

    public void setWatches(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches,
            Watcher watcher) {
        for (String path : dataWatches) {
            DataNode node = getNode(path);
            WatchedEvent e = null;
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                            KeeperState.SyncConnected, path));
            } else if (node.stat.getMzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeDataChanged,
                            KeeperState.SyncConnected, path));
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : existWatches) {
            DataNode node = getNode(path);
            if (node != null) {
                watcher.process(new WatchedEvent(EventType.NodeCreated,
                            KeeperState.SyncConnected, path));
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : childWatches) {
            DataNode node = getNode(path);
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                            KeeperState.SyncConnected, path));
            } else if (node.stat.getPzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeChildrenChanged,
                            KeeperState.SyncConnected, path));
            } else {
                this.childWatches.addWatch(path, watcher);
            }
        }
    }

     /**
      * This method sets the Cversion and Pzxid for the specified node to the
      * values passed as arguments. The values are modified only if newCversion
      * is greater than the current Cversion. A NoNodeException is thrown if
      * a znode for the specified path is not found.
      *
      * @param path
      *     Full path to the znode whose Cversion needs to be modified.
      *     A "/" at the end of the path is ignored.
      * @param newCversion
      *     Value to be assigned to Cversion
      * @param zxid
      *     Value to be assigned to Pzxid
      * @throws KeeperException.NoNodeException
      *     If znode not found.
      **/
    public void setCversionPzxid(String path, int newCversion, long zxid)
        throws KeeperException.NoNodeException {
        if (path.endsWith("/")) {
           path = path.substring(0, path.length() - 1);
        }
        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException(path);
        }
        synchronized (node) {
            if(newCversion == -1) {
                newCversion = node.stat.getCversion() + 1;
            }
            if (newCversion > node.stat.getCversion()) {
                node.stat.setCversion(newCversion);
                node.stat.setPzxid(zxid);
            }
        }
    }

    public boolean containsWatcher(String path, WatcherType type, Watcher watcher) {
        boolean containsWatcher = false;
        switch (type) {
        case Children:
            containsWatcher = this.childWatches.containsWatcher(path, watcher);
            break;
        case Data:
            containsWatcher = this.dataWatches.containsWatcher(path, watcher);
            break;
        case Any:
            if (this.childWatches.containsWatcher(path, watcher)) {
                containsWatcher = true;
            }
            if (this.dataWatches.containsWatcher(path, watcher)) {
                containsWatcher = true;
            }
            break;
        }
        return containsWatcher;
    }

    public boolean removeWatch(String path, WatcherType type, Watcher watcher) {
        boolean removed = false;
        switch (type) {
        case Children:
            removed = this.childWatches.removeWatcher(path, watcher);
            break;
        case Data:
            removed = this.dataWatches.removeWatcher(path, watcher);
            break;
        case Any:
            if (this.childWatches.removeWatcher(path, watcher)) {
                removed = true;
            }
            if (this.dataWatches.removeWatcher(path, watcher)) {
                removed = true;
            }
            break;
        }
        return removed;
    }

    // visible for testing
    public ReferenceCountedACLCache getReferenceCountedAclCache() {
        return aclCache;
    }
}
