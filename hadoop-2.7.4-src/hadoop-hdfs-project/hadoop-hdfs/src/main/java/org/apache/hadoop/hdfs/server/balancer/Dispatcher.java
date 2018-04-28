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
package org.apache.hadoop.hdfs.server.balancer;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/** 
 * Dispatching block replica moves between datanodes. 
 * 块副本移动
 */
@InterfaceAudience.Private
public class Dispatcher {
  static final Log LOG = LogFactory.getLog(Dispatcher.class);

  /**
   * 
   */
  private static long delayAfterErrors = 10 * 1000;

  private final NameNodeConnector nnc;
  private final SaslDataTransferClient saslClient;

  /** Set of datanodes to be excluded. 
   * 要排除的DataNode集
   * */
  private final Set<String> excludedNodes;
  /** Restrict to the following nodes. */
  private final Set<String> includedNodes;

  private final Collection<Source> sources = new HashSet<Source>();
  private final Collection<StorageGroup> targets = new HashSet<StorageGroup>();

  private final GlobalBlockMap globalBlocks = new GlobalBlockMap();
  private final MovedBlocks<StorageGroup> movedBlocks;

  /** Map (datanodeUuid,storageType -> StorageGroup) */
  private final StorageGroupMap<StorageGroup> storageGroupMap
      = new StorageGroupMap<StorageGroup>();

  private NetworkTopology cluster;

  private final ExecutorService dispatchExecutor;

  private final Allocator moverThreadAllocator;

  /** The maximum number of concurrent blocks moves at a datanode */
  private final int maxConcurrentMovesPerNode;
  private final int maxMoverThreads;

  private final long getBlocksSize;
  private final long getBlocksMinBlockSize;
  private final long blockMoveTimeout;
  /**
   * If no block can be moved out of a {@link Source} after this configured
   * amount of time, the Source should give up choosing the next possible move.
   */
  private final int maxNoMoveInterval;

  static class Allocator {
    private final int max;
    private int count = 0;
    private int lotSize = 1;

    Allocator(int max) {
      this.max = max;
    }

    /** Allocate specified number of items 
     * 分配指定数量的项目
     * */
    synchronized int allocate(int n) {
      final int remaining = max - count;
      if (remaining <= 0) {
        return 0;
      } else {
        final int allocated = remaining < n? remaining: n;
        count += allocated;
        return allocated;
      }
    }

    /** 
     * Aloocate a single lot of items 
     * 
     */
    int allocate() {
      return allocate(lotSize);
    }

    synchronized void reset() {
      count = 0;
    }

    /** Set the lot size */
    synchronized void setLotSize(int lotSize) {
      this.lotSize = lotSize;
    }
  }

  private static class GlobalBlockMap {
    private final Map<Block, DBlock> map = new HashMap<Block, DBlock>();

    /**
     * Get the block from the map;
     * if the block is not found, create a new block and put it in the map.
     */
    private DBlock get(Block b) {
      DBlock block = map.get(b);
      if (block == null) {
        block = new DBlock(b);
        map.put(b, block);
      }
      return block;
    }
    
    /** Remove all blocks except for the moved blocks. 
     * 移除所有不需要移动的数据块
     * */
    private void removeAllButRetain(MovedBlocks<StorageGroup> movedBlocks) {
      for (Iterator<Block> i = map.keySet().iterator(); i.hasNext();) {
        if (!movedBlocks.contains(i.next())) {
          i.remove();
        }
      }
    }
  }

  public static class StorageGroupMap<G extends StorageGroup> {
    private static String toKey(String datanodeUuid, StorageType storageType) {
      return datanodeUuid + ":" + storageType;
    }

    private final Map<String, G> map = new HashMap<String, G>();

    public G get(String datanodeUuid, StorageType storageType) {
      return map.get(toKey(datanodeUuid, storageType));
    }

    public void put(G g) {
      final String key = toKey(g.getDatanodeInfo().getDatanodeUuid(), g.storageType);
      final StorageGroup existing = map.put(key, g);
      Preconditions.checkState(existing == null);
    }

    int size() {
      return map.size();
    }

    void clear() {
      map.clear();
    }

    public Collection<G> values() {
      return map.values();
    }
  }

  /** This class keeps track of a scheduled block move */
  public class PendingMove {
    private DBlock block;
    private Source source;
    private DDatanode proxySource;
    private StorageGroup target;

    private PendingMove(Source source, StorageGroup target) {
      this.source = source;
      this.target = target;
    }

    @Override
    public String toString() {
      final Block b = block != null ? block.getBlock() : null;
      String bStr = b != null ? (b + " with size=" + b.getNumBytes() + " ")
          : " ";
      return bStr + "from " + source.getDisplayName() + " to " + target
          .getDisplayName() + " through " + (proxySource != null ? proxySource
          .datanode : "");
    }

    /**
     * Choose a block & a proxy source for this pendingMove whose source &
     * target have already been chosen.
     * 
     * @return true if a block and its proxy are chosen; false otherwise
     */
    private boolean chooseBlockAndProxy() {
      // source and target must have the same storage type
      final StorageType t = source.getStorageType();
      // iterate all source's blocks until find a good one
      for (Iterator<DBlock> i = source.getBlockIterator(); i.hasNext();) {
        if (markMovedIfGoodBlock(i.next(), t)) {
          i.remove();
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if the given block is good for the tentative move.
     */
    private boolean markMovedIfGoodBlock(DBlock block, StorageType targetStorageType) {
      synchronized (block) {
        synchronized (movedBlocks) {
          if (isGoodBlockCandidate(source, target, targetStorageType, block)) {
            this.block = block;
            if (chooseProxySource()) {
              movedBlocks.put(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move " + this);
              }
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Choose a proxy source.
     * 
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      final DatanodeInfo targetDN = target.getDatanodeInfo();
      // if source and target are same nodes then no need of proxy
      if (source.getDatanodeInfo().equals(targetDN) && addTo(source)) {
        return true;
      }
      // if node group is supported, first try add nodes in the same node group
      if (cluster.isNodeGroupAware()) {
        for (StorageGroup loc : block.getLocations()) {
          if (cluster.isOnSameNodeGroup(loc.getDatanodeInfo(), targetDN)
              && addTo(loc)) {
            return true;
          }
        }
      }
      // check if there is replica which is on the same rack with the target
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), targetDN) && addTo(loc)) {
          return true;
        }
      }
      // find out a non-busy replica
      for (StorageGroup loc : block.getLocations()) {
        if (addTo(loc)) {
          return true;
        }
      }
      return false;
    }

    /** add to a proxy source for specific block movement */
    private boolean addTo(StorageGroup g) {
      final DDatanode dn = g.getDDatanode();
      if (dn.addPendingBlock(this)) {
        proxySource = dn;
        return true;
      }
      return false;
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    private void dispatch() {
      LOG.info("Start moving " + this);

      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(target.getDatanodeInfo().getXferAddr()),
            HdfsServerConstants.READ_TIMEOUT);

        // Set read timeout so that it doesn't hang forever against
        // unresponsive nodes. Datanode normally sends IN_PROGRESS response
        // twice within the client read timeout period (every 30 seconds by
        // default). Here, we make it give up after 5 minutes of no response.
        sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT * 5);
        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
            block.getBlock());
        final KeyManager km = nnc.getKeyManager(); 
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
            unbufIn, km, accessToken, target.getDatanodeInfo());
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsConstants.IO_FILE_BUFFER_SIZE));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);
        nnc.getBytesMoved().addAndGet(block.getNumBytes());
        LOG.info("Successfully moved " + this);
      } catch (IOException e) {
        LOG.warn("Failed to move " + this + ": " + e.getMessage());
        target.getDDatanode().setHasFailure();
        // Proxy or target may have some issues, delay before using these nodes
        // further in order to avoid a potential storm of "threads quota
        // exceeded" warnings when the dispatcher gets out of sync with work
        // going on in datanodes.
        proxySource.activateDelay(delayAfterErrors);
        target.getDDatanode().activateDelay(delayAfterErrors);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);

        proxySource.removePendingBlock(this);
        target.getDDatanode().removePendingBlock(this);

        synchronized (this) {
          reset();
        }
        synchronized (Dispatcher.this) {
          Dispatcher.this.notifyAll();
        }
      }
    }

    /** Send a block replace request to the output stream */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken) throws IOException {
      new Sender(out).replaceBlock(eb, target.storageType, accessToken,
          source.getDatanodeInfo().getDatanodeUuid(), proxySource.datanode);
    }

    /** Check whether to continue waiting for response */
    private boolean stopWaitingForResponse(long startTime) {
      return source.isIterationOver() ||
          (blockMoveTimeout > 0 &&
          (Time.monotonicNow() - startTime > blockMoveTimeout));
    }

    /** Receive a reportedBlock copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      long startTime = Time.monotonicNow();
      BlockOpResponseProto response =
          BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
        // Stop waiting for slow block moves. Even if it stops waiting,
        // the actual move may continue.
        if (stopWaitingForResponse(startTime)) {
          throw new IOException("Block move timed out");
        }
      }
      String logInfo = "block move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
    }

    /** reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }
  }

  /** A class for keeping track of block locations in the dispatcher. */
  public static class DBlock extends MovedBlocks.Locations<StorageGroup> {
    public DBlock(Block block) {
      super(block);
    }
  }

  /** The class represents a desired move. */
  static class Task {
    private final StorageGroup target;
    private long size; // bytes scheduled to move

    Task(StorageGroup target, long size) {
      this.target = target;
      this.size = size;
    }

    long getSize() {
      return size;
    }
  }

  /** A class that keeps track of a datanode. */
  public static class DDatanode {

    /** A group of storages in a datanode with the same storage type. */
    public class StorageGroup {
      final StorageType storageType;
      final long maxSize2Move;
      private long scheduledSize = 0L;

      private StorageGroup(StorageType storageType, long maxSize2Move) {
        this.storageType = storageType;
        this.maxSize2Move = maxSize2Move;
      }
      
      public StorageType getStorageType() {
        return storageType;
      }

      private DDatanode getDDatanode() {
        return DDatanode.this;
      }

      public DatanodeInfo getDatanodeInfo() {
        return DDatanode.this.datanode;
      }

      /** Decide if still need to move more bytes */
      boolean hasSpaceForScheduling() {
        return hasSpaceForScheduling(0L);
      }

      synchronized boolean hasSpaceForScheduling(long size) {
        return availableSizeToMove() > size;
      }

      /** @return the total number of bytes that need to be moved */
      synchronized long availableSizeToMove() {
        return maxSize2Move - scheduledSize;
      }

      /** increment scheduled size */
      public synchronized void incScheduledSize(long size) {
        scheduledSize += size;
      }

      /** @return scheduled size */
      synchronized long getScheduledSize() {
        return scheduledSize;
      }

      /** Reset scheduled size to zero. */
      synchronized void resetScheduledSize() {
        scheduledSize = 0L;
      }

      private PendingMove addPendingMove(DBlock block, final PendingMove pm) {
        if (getDDatanode().addPendingBlock(pm)) {
          if (pm.markMovedIfGoodBlock(block, getStorageType())) {
            incScheduledSize(pm.block.getNumBytes());
            return pm;
          } else {
            getDDatanode().removePendingBlock(pm);
          }
        }
        return null;
      }

      /** @return the name for display */
      String getDisplayName() {
        return datanode + ":" + storageType;
      }

      @Override
      public String toString() {
        return getDisplayName();
      }

      @Override
      public int hashCode() {
        return getStorageType().hashCode() ^ getDatanodeInfo().hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        } else if (obj == null || !(obj instanceof StorageGroup)) {
          return false;
        } else {
          final StorageGroup that = (StorageGroup) obj;
          return this.getStorageType() == that.getStorageType()
              && this.getDatanodeInfo().equals(that.getDatanodeInfo());
        }
      }

    }

    final DatanodeInfo datanode;
    private final EnumMap<StorageType, Source> sourceMap
        = new EnumMap<StorageType, Source>(StorageType.class);
    private final EnumMap<StorageType, StorageGroup> targetMap
        = new EnumMap<StorageType, StorageGroup>(StorageType.class);
    protected long delayUntil = 0L;
    /** blocks being moved but not confirmed yet */
    private final List<PendingMove> pendings;
    private volatile boolean hasFailure = false;
    private ExecutorService moveExecutor;

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + datanode;
    }

    private DDatanode(DatanodeInfo datanode, int maxConcurrentMoves) {
      this.datanode = datanode;
      this.pendings = new ArrayList<PendingMove>(maxConcurrentMoves);
    }

    public DatanodeInfo getDatanodeInfo() {
      return datanode;
    }

    synchronized ExecutorService initMoveExecutor(int poolSize) {
      return moveExecutor = Executors.newFixedThreadPool(poolSize);
    }

    synchronized ExecutorService getMoveExecutor() {
      return moveExecutor;
    }

    synchronized void shutdownMoveExecutor() {
      if (moveExecutor != null) {
        moveExecutor.shutdown();
        moveExecutor = null;
      }
    }

    private static <G extends StorageGroup> void put(StorageType storageType,
        G g, EnumMap<StorageType, G> map) {
      final StorageGroup existing = map.put(storageType, g);
      Preconditions.checkState(existing == null);
    }

    public StorageGroup addTarget(StorageType storageType, long maxSize2Move) {
      final StorageGroup g = new StorageGroup(storageType, maxSize2Move);
      put(storageType, g, targetMap);
      return g;
    }

    public Source addSource(StorageType storageType, long maxSize2Move, Dispatcher d) {
      final Source s = d.new Source(storageType, maxSize2Move, this);
      put(storageType, s, sourceMap);
      return s;
    }

    synchronized private void activateDelay(long delta) {
      delayUntil = Time.monotonicNow() + delta;
      LOG.info(this + " activateDelay " + delta/1000.0 + " seconds");
    }

    synchronized private boolean isDelayActive() {
      if (delayUntil == 0 || Time.monotonicNow() > delayUntil) {
        delayUntil = 0;
        return false;
      }
      return true;
    }

    /** Check if all the dispatched moves are done */
    synchronized boolean isPendingQEmpty() {
      return pendings.isEmpty();
    }

    /** Add a scheduled block move to the node */
    synchronized boolean addPendingBlock(PendingMove pendingBlock) {
      if (!isDelayActive()) {
        return pendings.add(pendingBlock);
      }
      return false;
    }

    /** Remove a scheduled block move from the node */
    synchronized boolean removePendingBlock(PendingMove pendingBlock) {
      return pendings.remove(pendingBlock);
    }

    void setHasFailure() {
      this.hasFailure = true;
    }
  }

  /** A node that can be the sources of a block move */
  public class Source extends DDatanode.StorageGroup {

    private final List<Task> tasks = new ArrayList<Task>(2);
    private long blocksToReceive = 0L;
    private final long startTime = Time.monotonicNow();
    /**
     * Source blocks point to the objects in {@link Dispatcher#globalBlocks}
     * because we want to keep one copy of a block and be aware that the
     * locations are changing over time.
     */
    private final List<DBlock> srcBlocks = new ArrayList<DBlock>();

    private Source(StorageType storageType, long maxSize2Move, DDatanode dn) {
      dn.super(storageType, maxSize2Move);
    }

    /**
     * Check if the iteration is over
     */
    public boolean isIterationOver() {
      return (Time.monotonicNow()-startTime > MAX_ITERATION_TIME);
    }

    /** Add a task */
    void addTask(Task task) {
      Preconditions.checkState(task.target != this,
          "Source and target are the same storage group " + getDisplayName());
      incScheduledSize(task.size);
      tasks.add(task);
    }

    /** @return an iterator to this source's blocks */
    Iterator<DBlock> getBlockIterator() {
      return srcBlocks.iterator();
    }

    /**
     * Fetch new blocks of this source from namenode and update this source's
     * block list & {@link Dispatcher#globalBlocks}.
     * 
     * @return the total size of the received blocks in the number of bytes.
     */
    private long getBlockList() throws IOException {
      final long size = Math.min(getBlocksSize, blocksToReceive);
      final BlocksWithLocations newBlocks = nnc.getBlocks(getDatanodeInfo(), size);

      if (LOG.isTraceEnabled()) {
        LOG.trace("getBlocks(" + getDatanodeInfo() + ", "
            + StringUtils.TraditionalBinaryPrefix.long2String(size, "B", 2)
            + ") returns " + newBlocks.getBlocks().length + " blocks.");
      }

      long bytesReceived = 0;
      for (BlockWithLocations blk : newBlocks.getBlocks()) {
        // Skip small blocks.
        if (blk.getBlock().getNumBytes() < getBlocksMinBlockSize) {
          continue;
        }

        bytesReceived += blk.getBlock().getNumBytes();
        synchronized (globalBlocks) {
          final DBlock block = globalBlocks.get(blk.getBlock());
          synchronized (block) {
            block.clearLocations();

            // update locations
            final String[] datanodeUuids = blk.getDatanodeUuids();
            final StorageType[] storageTypes = blk.getStorageTypes();
            for (int i = 0; i < datanodeUuids.length; i++) {
              final StorageGroup g = storageGroupMap.get(
                  datanodeUuids[i], storageTypes[i]);
              if (g != null) { // not unknown
                block.addLocation(g);
              }
            }
          }
          if (!srcBlocks.contains(block) && isGoodBlockCandidate(block)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Add " + block + " to " + this);
            }
            srcBlocks.add(block);
          }
        }
      }
      return bytesReceived;
    }

    /** 
     * Decide if the given block is a good candidate to move or not
     * 检查
     * */
    private boolean isGoodBlockCandidate(DBlock block) {
      // source and target must have the same storage type
      final StorageType sourceStorageType = getStorageType();
      for (Task t : tasks) {
        if (Dispatcher.this.isGoodBlockCandidate(this, t.target,
            sourceStorageType, block)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Choose a move for the source. The block's source, target, and proxy
     * are determined too. When choosing proxy and target, source &
     * target throttling has been considered. They are chosen only when they
     * have the capacity to support this block move. The block should be
     * dispatched immediately after this method is returned.
     * 
     * @return a move that's good for the source to dispatch immediately.
     */
    private PendingMove chooseNextMove() {
      for (Iterator<Task> i = tasks.iterator(); i.hasNext();) {
        final Task task = i.next();
        final DDatanode target = task.target.getDDatanode();
        final PendingMove pendingBlock = new PendingMove(this, task.target);
        if (target.addPendingBlock(pendingBlock)) {
          // target is not busy, so do a tentative block allocation
          if (pendingBlock.chooseBlockAndProxy()) {
            long blockSize = pendingBlock.block.getNumBytes();
            incScheduledSize(-blockSize);
            task.size -= blockSize;
            if (task.size <= 0) {
              i.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }
    
    /** Add a pending move */
    public PendingMove addPendingMove(DBlock block, StorageGroup target) {
      return target.addPendingMove(block, new PendingMove(this, target));
    }

    /** Iterate all source's blocks to remove moved ones */
    private void removeMovedBlocks() {
      for (Iterator<DBlock> i = getBlockIterator(); i.hasNext();) {
        if (movedBlocks.contains(i.next().getBlock())) {
          i.remove();
        }
      }
    }

    /** @return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return blocksToReceive > 0;
    }

    private static final long MAX_ITERATION_TIME = 20 * 60 * 1000L; // 20 mins

    /**
     * This method iteratively does the following: it first selects a block to
     * move, then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks the
     * namenode for more blocks. It terminates when it has dispatch enough block
     * move tasks or it has received enough blocks from the namenode, or the
     * elapsed time of the iteration has exceeded the max time limit.
     *
     * @param delay - time to sleep before sending getBlocks. Intended to
     * disperse Balancer RPCs to NameNode for large clusters. See HDFS-11384.
     */
    private void dispatchBlocks(long delay) {
      this.blocksToReceive = 2 * getScheduledSize();
      long previousMoveTimestamp = Time.monotonicNow();
      while (getScheduledSize() > 0 && !isIterationOver()
          && (!srcBlocks.isEmpty() || blocksToReceive > 0)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + " blocksToReceive=" + blocksToReceive
              + ", scheduledSize=" + getScheduledSize()
              + ", srcBlocks#=" + srcBlocks.size());
        }
        final PendingMove p = chooseNextMove();
        if (p != null) {
          // Reset previous move timestamp
          previousMoveTimestamp = Time.monotonicNow();
          executePendingMove(p);
          continue;
        }

        // Since we cannot schedule any block to move,
        // remove any moved blocks from the source block list and
        removeMovedBlocks(); // filter already moved blocks
        // check if we should fetch more blocks from the namenode
        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            if(delay > 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Sleeping " + delay + "  msec.");
              }
              Thread.sleep(delay);
            }
            blocksToReceive -= getBlockList();
            continue;
          } catch (InterruptedException ignored) {
            // nothing to do
          } catch (IOException e) {
            LOG.warn("Exception while getting block list", e);
            return;
          } finally {
            delay = 0L;
          }
        } else {
          // jump out of while-loop after the configured timeout.
          long noMoveInterval = Time.monotonicNow() - previousMoveTimestamp;
          if (noMoveInterval > maxNoMoveInterval) {
            LOG.info("Failed to find a pending move for "  + noMoveInterval
                + " ms.  Skipping " + this);
            resetScheduledSize();
          }
        }

        // Now we can not schedule any block to move and there are
        // no new blocks added to the source block list, so we wait.
        try {
          synchronized (Dispatcher.this) {
            Dispatcher.this.wait(1000); // wait for targets/sources to be idle
          }
          // Didn't find a possible move in this iteration of the while loop,
          // adding a small delay before choosing next move again.
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
      }

      if (isIterationOver()) {
        LOG.info("The maximum iteration time (" + MAX_ITERATION_TIME/1000
            + " seconds) has been reached. Stopping " + this);
      }
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }

  /** Constructor called by Mover. */
  public Dispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth, int moverThreads,
      int dispatcherThreads, int maxConcurrentMovesPerNode,
      int maxNoMoveInterval, Configuration conf) {
    this(nnc, includedNodes, excludedNodes, movedWinWidth,
        moverThreads, dispatcherThreads, maxConcurrentMovesPerNode,
        0L, 0L, 0, maxNoMoveInterval, conf);
  }

  Dispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth, int moverThreads,
      int dispatcherThreads, int maxConcurrentMovesPerNode,
      long getBlocksSize, long getBlocksMinBlockSize,
      int blockMoveTimeout, int maxNoMoveInterval, Configuration conf) {
    this.nnc = nnc;
    this.excludedNodes = excludedNodes;
    this.includedNodes = includedNodes;
    this.movedBlocks = new MovedBlocks<StorageGroup>(movedWinWidth);

    this.cluster = NetworkTopology.getInstance(conf);

    this.dispatchExecutor = dispatcherThreads == 0? null
        : Executors.newFixedThreadPool(dispatcherThreads);
    this.moverThreadAllocator = new Allocator(moverThreads);
    this.maxMoverThreads = moverThreads;
    this.maxConcurrentMovesPerNode = maxConcurrentMovesPerNode;

    this.getBlocksSize = getBlocksSize;
    this.getBlocksMinBlockSize = getBlocksMinBlockSize;
    this.blockMoveTimeout = blockMoveTimeout;
    this.maxNoMoveInterval = maxNoMoveInterval;

    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
  }

  public DistributedFileSystem getDistributedFileSystem() {
    return nnc.getDistributedFileSystem();
  }

  public StorageGroupMap<StorageGroup> getStorageGroupMap() {
    return storageGroupMap;
  }

  public NetworkTopology getCluster() {
    return cluster;
  }
  
  long getBytesMoved() {
    return nnc.getBytesMoved().get();
  }

  long bytesToMove() {
    Preconditions.checkState(
        storageGroupMap.size() >= sources.size() + targets.size(),
        "Mismatched number of storage groups (" + storageGroupMap.size()
            + " < " + sources.size() + " sources + " + targets.size()
            + " targets)");

    long b = 0L;
    for (Source src : sources) {
      b += src.getScheduledSize();
    }
    return b;
  }

  void add(Source source, StorageGroup target) {
    sources.add(source);
    targets.add(target);
  }

  private boolean shouldIgnore(DatanodeInfo dn) {
    // ignore decommissioned nodes
    final boolean decommissioned = dn.isDecommissioned();
    // ignore decommissioning nodes
    final boolean decommissioning = dn.isDecommissionInProgress();
    // ignore nodes in exclude list
    final boolean excluded = Util.isExcluded(excludedNodes, dn);
    // ignore nodes not in the include list (if include list is not empty)
    final boolean notIncluded = !Util.isIncluded(includedNodes, dn);

    if (decommissioned || decommissioning || excluded || notIncluded) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Excluding datanode " + dn + ": " + decommissioned + ", "
            + decommissioning + ", " + excluded + ", " + notIncluded);
      }
      return true;
    }
    return false;
  }

  /** Get live datanode storage reports and then build the network topology. */
  public List<DatanodeStorageReport> init() throws IOException {
    final DatanodeStorageReport[] reports = nnc.getLiveDatanodeStorageReport();
    final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>(); 
    // create network topology and classify utilization collections:
    // over-utilized, above-average, below-average and under-utilized.
    for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      if (shouldIgnore(datanode)) {
        continue;
      }
      trimmed.add(r);
      cluster.add(datanode);
    }
    return trimmed;
  }

  public DDatanode newDatanode(DatanodeInfo datanode) {
    return new DDatanode(datanode, maxConcurrentMovesPerNode);
  }


  public void executePendingMove(final PendingMove p) {
    // move the block
    final DDatanode targetDn = p.target.getDDatanode();
    ExecutorService moveExecutor = targetDn.getMoveExecutor();
    if (moveExecutor == null) {
      final int nThreads = moverThreadAllocator.allocate();
      if (nThreads > 0) {
        moveExecutor = targetDn.initMoveExecutor(nThreads);
      }
    }
    if (moveExecutor == null) {
      LOG.warn("No mover threads available: skip moving " + p);
      targetDn.removePendingBlock(p);
      p.proxySource.removePendingBlock(p);
      return;
    }

    moveExecutor.execute(new Runnable() {
      @Override
      public void run() {
        p.dispatch();
      }
    });
  }

  public boolean dispatchAndCheckContinue() throws InterruptedException {
    return nnc.shouldContinue(dispatchBlockMoves());
  }

  /**
   * The best-effort limit on the number of RPCs per second
   * the Balancer will send to the NameNode.
   */
  final static int BALANCER_NUM_RPC_PER_SEC = 20;

  /**
   * Dispatch block moves for each source. The thread selects blocks to move &
   * sends request to proxy source to initiate block move. The process is flow
   * controlled. Block selection is blocked if there are too many un-confirmed
   * block moves.
   * 
   * @return the total number of bytes successfully moved in this iteration.
   */
  private long dispatchBlockMoves() throws InterruptedException {
    final long bytesLastMoved = getBytesMoved();
    final Future<?>[] futures = new Future<?>[sources.size()];

    int concurrentThreads = Math.min(sources.size(),
        ((ThreadPoolExecutor)dispatchExecutor).getCorePoolSize());
    assert concurrentThreads > 0 : "Number of concurrent threads is 0.";
    if (LOG.isDebugEnabled()) {
      LOG.debug("Balancer allowed RPCs per sec = " + BALANCER_NUM_RPC_PER_SEC);
      LOG.debug("Balancer concurrent threads = " + concurrentThreads);
      LOG.debug("Disperse Interval sec = " +
          concurrentThreads / BALANCER_NUM_RPC_PER_SEC);
    }

    // Determine the size of each mover thread pool per target
    int threadsPerTarget = maxMoverThreads/targets.size();
    if (threadsPerTarget == 0) {
      // Some scheduled moves will get ignored as some targets won't have
      // any threads allocated.
      moverThreadAllocator.setLotSize(1);
      LOG.warn(DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY + "=" +
          maxMoverThreads + " is too small for moving blocks to " +
          targets.size() + " targets. Balancing may be slower.");
    } else {
      if  (threadsPerTarget > maxConcurrentMovesPerNode) {
        threadsPerTarget = maxConcurrentMovesPerNode;
        LOG.info("Limiting threads per target to the specified max.");
      }
      moverThreadAllocator.setLotSize(threadsPerTarget);
      LOG.info("Allocating " + threadsPerTarget + " threads per target.");
    }

    long dSec = 0;
    final Iterator<Source> i = sources.iterator();
    for (int j = 0; j < futures.length; j++) {
      final Source s = i.next();
      final long delay = dSec * 1000;
      futures[j] = dispatchExecutor.submit(new Runnable() {
        @Override
        public void run() {
          s.dispatchBlocks(delay);
        }
      });
      // Calculate delay in seconds for the next iteration
      if(j >= concurrentThreads) {
        dSec = 0;
      } else if((j + 1) % BALANCER_NUM_RPC_PER_SEC == 0) {
        dSec++;
      }
    }

    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }

    // wait for all block moving to be done
    waitForMoveCompletion(targets);

    return getBytesMoved() - bytesLastMoved;
  }

  /**
   * Wait for all block move confirmations.
   * @return true if there is failed move execution
   */
  public static boolean waitForMoveCompletion(
      Iterable<? extends StorageGroup> targets) {
    boolean hasFailure = false;
    for(;;) {
      boolean empty = true;
      for (StorageGroup t : targets) {
        if (!t.getDDatanode().isPendingQEmpty()) {
          empty = false;
          break;
        } else {
          hasFailure |= t.getDDatanode().hasFailure;
        }
      }
      if (empty) {
        return hasFailure; // all pending queues are empty
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }
    }
  }

  /**
   * Decide if the block is a good candidate to be moved from source to target.
   * A block is a good candidate if
   * 1. the block is not in the process of being moved/has not been moved;
   * 移动的块不是正在被移动的块
   * 2. the block does not have a replica on the target;
   * 在目标节点上没有移动的block块副本
   * 3. doing the move does not reduce the number of racks that the block has
   * 移动之后，不同机架上block块的数量应该是不变的
   */
  private boolean isGoodBlockCandidate(StorageGroup source, StorageGroup target,
      StorageType targetStorageType, DBlock block) {
    if (source.equals(target)) {
      return false;
    }
    if (target.storageType != targetStorageType) {
      return false;
    }
    // check if the block is moved or not
    //如果所要移动的块处在正在被移动块列表中，则返回false
    if (movedBlocks.contains(block.getBlock())) {
      return false;
    }
    final DatanodeInfo targetDatanode = target.getDatanodeInfo();
    if (source.getDatanodeInfo().equals(targetDatanode)) {
      // the block is moved inside same DN
      return true;
    }

    // check if block has replica in target node
    //检查目标节点是否有要移动块的副本
    for (StorageGroup blockLocation : block.getLocations()) {
      if (blockLocation.getDatanodeInfo().equals(targetDatanode)) {
        return false;
      }
    }

  //如果开启了机架感知的配置,则目标节点不应该有相同的block
    if (cluster.isNodeGroupAware()
        && isOnSameNodeGroupWithReplicas(source, target, block)) {
      return false;
    }
  //需要维持机架上的block块数量不变  
    if (reduceNumOfRacks(source, target, block)) {
      return false;
    }
    return true;
  }

  /**
   * Determine whether moving the given block replica from source to target
   * would reduce the number of racks of the block replicas.
   */
  private boolean reduceNumOfRacks(StorageGroup source, StorageGroup target,
      DBlock block) {
    final DatanodeInfo sourceDn = source.getDatanodeInfo();
    if (cluster.isOnSameRack(sourceDn, target.getDatanodeInfo())) {
      // source and target are on the same rack
      return false;
    }
    boolean notOnSameRack = true;
    synchronized (block) {
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), target.getDatanodeInfo())) {
          notOnSameRack = false;
          break;
        }
      }
    }
    if (notOnSameRack) {
      // target is not on the same rack as any replica
      return false;
    }
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameRack(g.getDatanodeInfo(), sourceDn)) {
        // source is on the same rack of another replica
        return false;
      }
    }
    return true;
  }

  /**
   * Check if there are any replica (other than source) on the same node group
   * with target. If true, then target is not a good candidate for placing
   * specific replica as we don't want 2 replicas under the same nodegroup.
   *
   * @return true if there are any replica (other than source) on the same node
   *         group with target
   */
  private boolean isOnSameNodeGroupWithReplicas(StorageGroup source,
      StorageGroup target, DBlock block) {
    final DatanodeInfo targetDn = target.getDatanodeInfo();
    for (StorageGroup g : block.getLocations()) {
      if (g != source && cluster.isOnSameNodeGroup(g.getDatanodeInfo(), targetDn)) {
        return true;
      }
    }
    return false;
  }

  /** Reset all fields in order to prepare for the next iteration */
  void reset(Configuration conf) {
    cluster = NetworkTopology.getInstance(conf);
    storageGroupMap.clear();
    sources.clear();

    moverThreadAllocator.reset();
    for(StorageGroup t : targets) {
      t.getDDatanode().shutdownMoveExecutor();
    }
    targets.clear();
    globalBlocks.removeAllButRetain(movedBlocks);
    movedBlocks.cleanup();
  }

  @VisibleForTesting
  public static void setDelayAfterErrors(long time) {
    delayAfterErrors = time;
  }

  /** shutdown thread pools */
  public void shutdownNow() {
    if (dispatchExecutor != null) {
      dispatchExecutor.shutdownNow();
    }
  }

  static class Util {
    /** @return true if data node is part of the excludedNodes. */
    static boolean isExcluded(Set<String> excludedNodes, DatanodeInfo dn) {
      return isIn(excludedNodes, dn);
    }

    /**
     * @return true if includedNodes is empty or data node is part of the
     *         includedNodes.
     */
    static boolean isIncluded(Set<String> includedNodes, DatanodeInfo dn) {
      return (includedNodes.isEmpty() || isIn(includedNodes, dn));
    }

    /**
     * Match is checked using host name , ip address with and without port
     * number.
     * 
     * @return true if the datanode's transfer address matches the set of nodes.
     */
    private static boolean isIn(Set<String> datanodes, DatanodeInfo dn) {
      return isIn(datanodes, dn.getPeerHostName(), dn.getXferPort())
          || isIn(datanodes, dn.getIpAddr(), dn.getXferPort())
          || isIn(datanodes, dn.getHostName(), dn.getXferPort());
    }

    /** @return true if nodes contains host or host:port */
    private static boolean isIn(Set<String> nodes, String host, int port) {
      if (host == null) {
        return false;
      }
      return (nodes.contains(host) || nodes.contains(host + ":" + port));
    }

    /**
     * Parse a comma separated string to obtain set of host names
     * 
     * @return set of host names
     */
    static Set<String> parseHostList(String string) {
      String[] addrs = StringUtils.getTrimmedStrings(string);
      return new HashSet<String>(Arrays.asList(addrs));
    }

    /**
     * Read set of host names from a file
     * 
     * @return set of host names
     */
    static Set<String> getHostListFromFile(String fileName, String type) {
      Set<String> nodes = new HashSet<String>();
      try {
        HostsFileReader.readFileToSet(type, fileName, nodes);
        return StringUtils.getTrimmedStrings(nodes);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to read host list from file: " + fileName);
      }
    }
  }
}