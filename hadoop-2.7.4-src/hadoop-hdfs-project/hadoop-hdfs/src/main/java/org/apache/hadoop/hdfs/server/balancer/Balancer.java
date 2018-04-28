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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Source;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Task;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.Util;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 *               bin/ start-balancer.sh -idleiterations 20
 *                     start the balancer with maximum 20 consecutive idle iterations
 *               bin/ start-balancer.sh -idleiterations -1
 *                     run the balancer with default threshold infinitely
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for specified consecutive iterations (5 by default);
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for specified iterations (5 by default). Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

@InterfaceAudience.Private
public class Balancer {
  static final Log LOG = LogFactory.getLog(Balancer.class);

  static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private static final String USAGE = "Usage: hdfs balancer"
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before "
      + "exit."
      + "\n\t[-runDuringUpgrade]"
      + "\tWhether to run the balancer during an ongoing HDFS upgrade."
      + "This is usually not desired since it will not affect used space "
      + "on over-utilized machines.";

  private final Dispatcher dispatcher;
  private final NameNodeConnector nnc;
  private final BalancingPolicy policy;
  private final boolean runDuringUpgrade;
  private final double threshold;
  private final long maxSizeToMove;

  // all data node lists
  private final Collection<Source> overUtilized = new LinkedList<Source>();
  private final Collection<Source> aboveAvgUtilized = new LinkedList<Source>();
  private final Collection<StorageGroup> belowAvgUtilized
      = new LinkedList<StorageGroup>();
  private final Collection<StorageGroup> underUtilized
      = new LinkedList<StorageGroup>();
  
  //修改：增加集合进行排序存储
//  private final LinkedList<Source> over = new LinkedList<Source>();
//  private final LinkedList<Source> above = new LinkedList<Source>();
//  private final LinkedList<StorageGroup> below = new LinkedList<StorageGroup>();
//  private final LinkedList<StorageGroup> under = new LinkedList<StorageGroup>();
  private final Map<Source,Double> over = new HashMap<Source,Double>();
  private final Map<Source,Double> above = new HashMap<Source,Double>();
  private final Map<StorageGroup,Double> below = new HashMap<StorageGroup,Double>();
  private final Map<StorageGroup,Double> under = new HashMap<StorageGroup,Double>();
  private static double utilizedTemp = 0;
  private static int temp = 0;
  
  /* Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.//检查该平衡器是否与Namenode使用的块放置策略兼容。
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
      ) throws UnsupportedActionException {
    if (!(BlockPlacementPolicy.getInstance(conf, null, null, null) instanceof 
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
    		//如果不兼容则抛异常 
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  static long getLong(Configuration conf, String key, long defaultValue) {
    final long v = conf.getLong(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  static int getInt(Configuration conf, String key, int defaultValue) {
    final int v = conf.getInt(key, defaultValue);
    LOG.info(key + " = " + v + " (default=" + defaultValue + ")");
    if (v <= 0) {
      throw new HadoopIllegalArgumentException(key + " = " + v  + " <= " + 0);
    }
    return v;
  }

  /**
   * Construct a balancer.
   * Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   * 构造一个balancer均衡器,设置threshold值,读取配置中的分发线程数的值 
   */
  Balancer(NameNodeConnector theblockpool, Parameters p, Configuration conf) {
	  //带宽
    final long movedWinWidth = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
  //移动线程数
    final int moverThreads = getInt(conf,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_DEFAULT);
  //分发线程数
    final int dispatcherThreads = getInt(conf,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_DEFAULT);
    //每个节点最大并发移动数量，默认值为5
    final int maxConcurrentMovesPerNode = getInt(conf,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    //得到块大小,默认2GB
    final long getBlocksSize = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_DEFAULT);
    //最小块大小,默认10MB
    final long getBlocksMinBlockSize = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_DEFAULT);
    //块移动超时时间限制
    final int blockMoveTimeout = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT,
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT);
    //最大没有移动数据时间间隔,默认1min
    final int maxNoMoveInterval = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY,
        DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_DEFAULT);
    
    //namenode连接池
    this.nnc = theblockpool;
    this.dispatcher = new Dispatcher(theblockpool, p.nodesToBeIncluded,
        p.nodesToBeExcluded, movedWinWidth, moverThreads, dispatcherThreads,
        maxConcurrentMovesPerNode, getBlocksSize, getBlocksMinBlockSize,
        blockMoveTimeout,maxNoMoveInterval, conf);
    this.threshold = p.threshold;
    this.policy = p.policy;
    
    //默认10GB(最大不超过10G)
    this.maxSizeToMove = getLong(conf,
        DFSConfigKeys.DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY,
        DFSConfigKeys.DFS_BALANCER_MAX_SIZE_TO_MOVE_DEFAULT);
    this.runDuringUpgrade = p.runDuringUpgrade;
  }
  
  /** 
   * 获取节点总容量大小 
   */   
  private static long getCapacity(DatanodeStorageReport report, StorageType t) {
    long capacity = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        capacity += r.getCapacity();
      }
    }
    return capacity;
  }

  /** 
   * 获取节点剩余可用容量大小  
   */  
  private static long getRemaining(DatanodeStorageReport report, StorageType t) {
    long remaining = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        remaining += r.getRemaining();
      }
    }
    return remaining;
  }

  /**
   * Given a datanode storage set, build a network topology and decide
   * over-utilized storages, above average utilized storages, 
   * below average utilized storages, and underutilized storages. 
   * The input datanode storage set is shuffled in order to randomize
   * to the storage matching later on.
   *
   * @return the number of bytes needed to move in order to balance the cluster.
   *给定一个datanode集合,创建一个网络拓扑逻辑并划分出过度使用,使用率超出平均值, 
   *低于平均值的,以及未能充分利用资源的4种类型 
   *返回平衡集群需要移动数据字节数
   */
  private long init(List<DatanodeStorageReport> reports) {
    // compute average utilization
    for (DatanodeStorageReport r : reports) {
      policy.accumulateSpaces(r);//累加节点的使用空间和总容量
    }
    policy.initAvgUtilization();//计算平均使用率

    // create network topology and classify utilization collections: 
    //   over-utilized, above-average, below-average and under-utilized.
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
  /*
   * 进行使用率等级的划分,总共4种:over-utilized, above-average, 
   * below-average and under-utilized
   */
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Double utilization = policy.getUtilization(r, t);
        if (utilization == null) { // datanode does not have such storage type 
          continue;
        }
        
        final long capacity = getCapacity(r, t);
        //utilizationDiff:用来判断是否在+-threshold之内
        final double utilizationDiff = utilization - policy.getAvgUtilization(t);
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = computeMaxSize2Move(capacity,
            getRemaining(r, t), utilizationDiff, threshold, maxSizeToMove);

        final StorageGroup g;
        //若utilizationDiff>0则此节点利用率高于平均值
        if (utilizationDiff > 0) {
          final Source s = dn.addSource(t, maxSize2Move, dispatcher);
          if (thresholdDiff <= 0) { // within threshold,在平衡区间内
        	above.put(s, utilization);
//            aboveAvgUtilized.add(s);
          } else {                   //高于平衡区间，是过载节点
            overLoadedBytes += percentage2bytes(thresholdDiff, capacity);//需要移动的字节数大小
            over.put(s, utilization);
//            overUtilized.add(s);
          }
          g = s;
        } else {     //utilizationDiff<0则此节点利用率低于平均值
          g = dn.addTarget(t, maxSize2Move);
          if (thresholdDiff <= 0) { // within threshold,在平衡区间内
        	below.put(g, utilization);
//            belowAvgUtilized.add(g);
          } else {                 //低于平衡区间，是负载节点
            underLoadedBytes += percentage2bytes(thresholdDiff, capacity);
            under.put(g, utilization);
//            underUtilized.add(g);
          }
        }
        dispatcher.getStorageGroupMap().put(g);
      }
    }

    List<Map.Entry<Source, Double>> list = sort(over);
    for (int i = list.size()-1; i >=0; i--) {
		overUtilized.add(list.get(i).getKey());
	}
    list = sort(above);
    for (int i = list.size()-1; i >=0; i--) {
		aboveAvgUtilized.add(list.get(i).getKey());
	}
    List<Map.Entry<StorageGroup, Double>> list1 = sort(under);
    for (int i = 0; i < list1.size(); i++) {
		underUtilized.add(list1.get(i).getKey());
	}
    list1 = sort(below);
    for (int i = 0; i < list1.size(); i++) {
		belowAvgUtilized.add(list1.get(i).getKey());
	}
    logUtilizationCollections();
    
    //Preconditions.checkState:校验表达式是否为真,表达式为假的时候，显示指定的错误信息。
    Preconditions.checkState(dispatcher.getStorageGroupMap().size()
        == overUtilized.size() + underUtilized.size() + aboveAvgUtilized.size()
           + belowAvgUtilized.size(),
        "Mismatched number of storage groups");
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  
  
  
  
  //排序，默认按照value升序排序
  private <K> List<Map.Entry<K, Double>> sort(Map<K,Double> map) {
//	  Set<T> set = map.keySet();
//	  Iterator<T> it = set.iterator();
	  Comparator<Map.Entry<K,Double>> valueComparator = new Comparator<Map.Entry<K,Double>>() {
			@Override
			public int compare(Entry<K,Double> o1, Entry<K,Double> o2) {
				double d= o1.getValue() - o2.getValue();
				if(d<0) {
					return -1;
				}
				return 1;
			}
		};
		// map转换成list进行排序
		List<Map.Entry<K,Double>> list = new ArrayList<Map.Entry<K,Double>>(map.entrySet());
		// 默认按照value值升序排序
		Collections.sort(list,valueComparator);
		return list;
  }
  
  //计算最大移动数据大小
  private static long computeMaxSize2Move(final long capacity, final long remaining,
      final double utilizationDiff, final double threshold, final long max) {
    final double diff = Math.min(threshold, Math.abs(utilizationDiff));
    long maxSizeToMove = percentage2bytes(diff, capacity);
    if (utilizationDiff < 0) {
      maxSizeToMove = Math.min(remaining, maxSizeToMove);
    }
    return Math.min(max, maxSizeToMove);
  }

  //计算需要移动的数据量
  private static long percentage2bytes(double percentage, long capacity) {
    Preconditions.checkArgument(percentage >= 0, "percentage = %s < 0",
        percentage);
    return (long)(percentage * capacity / 100.0);
  }

  /* log the over utilized & under utilized nodes */
  private void logUtilizationCollections() {
    logUtilizationCollection("over-utilized", overUtilized);
    if (LOG.isTraceEnabled()) {
      logUtilizationCollection("above-average", aboveAvgUtilized);
      logUtilizationCollection("below-average", belowAvgUtilized);
    }
    logUtilizationCollection("underutilized", underUtilized);
  }

  private static <T extends StorageGroup>
      void logUtilizationCollection(String name, Collection<T> items) {
    LOG.info(items.size() + " " + name + ": " + items);
  }

  /**
   * Decide all <source, target> pairs and
   * the number of bytes to move from a source to a target
   * 进行<source, target>对的匹配（选择顺序优先为同节点组,同机架,然后是针对所有）和计算需要移动的数据大小
   * Maximum bytes to be moved per storage group is
   * min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
   * @return total number of bytes to move in this iteration
   * 返回本次轮询移动的数据大小。
   * 
   */
  private long chooseStorageGroups() {
    // First, match nodes on the same node group if cluster is node group aware
    if (dispatcher.getCluster().isNodeGroupAware()) {
      chooseStorageGroups(Matcher.SAME_NODE_GROUP);
    }
    
    // Then, match nodes on the same rack
    chooseStorageGroups(Matcher.SAME_RACK);
    // At last, match all remaining nodes
    chooseStorageGroups(Matcher.ANY_OTHER);
    
    return dispatcher.bytesToMove();
  }

  /** Decide all <source, target> pairs according to the matcher. */
  private void chooseStorageGroups(final Matcher matcher) {
    /* first step: match each overUtilized datanode (source) to
     * one or more underUtilized datanodes (targets).
     */
    LOG.info("chooseStorageGroups for " + matcher + ": overUtilized => underUtilized");
    chooseStorageGroups(overUtilized, underUtilized, matcher);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    LOG.info("chooseStorageGroups for " + matcher + ": overUtilized => belowAvgUtilized");
    chooseStorageGroups(overUtilized, belowAvgUtilized, matcher);

    /* match each remaining underutilized datanode (target) to 
     * above average utilized datanodes (source).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    LOG.info("chooseStorageGroups for " + matcher + ": underUtilized => aboveAvgUtilized");
    chooseStorageGroups(underUtilized, aboveAvgUtilized, matcher);
  }

  /**
   * For each datanode, choose matching nodes from the candidates. Either the
   * datanodes or the candidates are source nodes with (utilization > Avg), and
   * the others are target nodes with (utilization < Avg).
   */
  private <G extends StorageGroup, C extends StorageGroup>
      void chooseStorageGroups(Collection<G> groups, Collection<C> candidates,
          Matcher matcher) {
    for(final Iterator<G> i = groups.iterator(); i.hasNext();) {
      final G g = i.next();
      for(; choose4One(g, candidates, matcher); );
      if (!g.hasSpaceForScheduling()) {
        i.remove();          //如果候选节点没有空间调度,则直接移除掉 
      }
    }
  }

  /**
   * For the given datanode, choose a candidate and then schedule it.
   * @return true if a candidate is chosen; false if no candidates is chosen.
   */
  private <C extends StorageGroup> boolean choose4One(StorageGroup g,
      Collection<C> candidates, Matcher matcher) {
    final Iterator<C> i = candidates.iterator();
    final C chosen = chooseCandidate(g, i, matcher);
  
    if (chosen == null) {
      return false;
    }
    if (g instanceof Source) {
      matchSourceWithTargetToMove((Source)g, chosen);
    } else {
      matchSourceWithTargetToMove((Source)chosen, g);
    }
    if (!chosen.hasSpaceForScheduling()) {
      i.remove();
    }
    return true;
  }
  
  
  //根据源节点和目标节点构造任务对
  private void matchSourceWithTargetToMove(Source source, StorageGroup target) {
	  //需要移动的数据大小
    long size = Math.min(source.availableSizeToMove(), target.availableSizeToMove());
    final Task task = new Task(target, size);
    source.addTask(task);
    target.incScheduledSize(task.getSize());
    //加入分发器中
    dispatcher.add(source, target);
    LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
        + source.getDisplayName() + " to " + target.getDisplayName());
  }
  
  /** Choose a candidate for the given datanode. */
  private <G extends StorageGroup, C extends StorageGroup>
      C chooseCandidate(G g, Iterator<C> candidates, Matcher matcher) {
    if (g.hasSpaceForScheduling()) {  //源节点是否还有需要移动的数据
      for(; candidates.hasNext(); ) {  //循环条件:还有候选节点(target)s
        final C c = candidates.next();
        if (!c.hasSpaceForScheduling()) {  
          candidates.remove();//如果候选节点没有空间调度，则直接移除
        } else if (matchStorageGroups(c, g, matcher)) {
          return c;
        }
      }
    }
    return null;
  }

  private boolean matchStorageGroups(StorageGroup left, StorageGroup right,
      Matcher matcher) {
    return left.getStorageType() == right.getStorageType()
        && matcher.match(dispatcher.getCluster(),
            left.getDatanodeInfo(), right.getDatanodeInfo());
  }

  /* reset all fields in a balancer preparing for the next iteration */
  void resetData(Configuration conf) {
    this.overUtilized.clear();
    this.aboveAvgUtilized.clear();
    this.belowAvgUtilized.clear();
    this.underUtilized.clear();
    this.policy.reset();
    dispatcher.reset(conf);;
  }

  static class Result {
    final ExitStatus exitStatus;
    final long bytesLeftToMove;
    final long bytesBeingMoved;
    final long bytesAlreadyMoved;

    Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,
        long bytesAlreadyMoved) {
      this.exitStatus = exitStatus;
      this.bytesLeftToMove = bytesLeftToMove;
      this.bytesBeingMoved = bytesBeingMoved;
      this.bytesAlreadyMoved = bytesAlreadyMoved;
    }

    void print(int iteration, PrintStream out) {
      out.printf("%-24s %10d  %19s  %18s  %17s%n",
          DateFormat.getDateTimeInstance().format(new Date()), iteration,
          StringUtils.byteDesc(bytesAlreadyMoved),
          StringUtils.byteDesc(bytesLeftToMove),
          StringUtils.byteDesc(bytesBeingMoved));
    }
  }

  Result newResult(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved) {
    return new Result(exitStatus, bytesLeftToMove, bytesBeingMoved,
        dispatcher.getBytesMoved());
  }

  Result newResult(ExitStatus exitStatus) {
    return new Result(exitStatus, -1, -1, dispatcher.getBytesMoved());
  }

  /** Run an iteration for all datanodes. */
  Result runOneIteration() {
    try {
      final List<DatanodeStorageReport> reports = dispatcher.init();
      final long bytesLeftToMove = init(reports);//使得集群平衡需要移动的数据大小
      if (bytesLeftToMove == 0) {
        System.out.println("The cluster is balanced. Exiting...");
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, -1);
      } else {
        LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
            + " to make the cluster balanced." );
      }

      // Should not run the balancer during an unfinalized upgrade, since moved
      // blocks are not deleted on the source datanode.
      if (!runDuringUpgrade && nnc.isUpgrading()) {
        return newResult(ExitStatus.UNFINALIZED_UPGRADE, bytesLeftToMove, -1);
      }

      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesBeingMoved = chooseStorageGroups();//每一次迭代需要移动的数据大小
      if (bytesBeingMoved == 0) {
        System.out.println("No block can be moved. Exiting...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved);
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesBeingMoved) +
            " in this iteration");
      }
      
      /* For each pair of <source, target>, start a thread that repeatedly 
       * decide a block to be moved and its proxy source, 
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       * 如果发现5次连续的迭代中发现没有字节移动则退出
       */
      if (!dispatcher.dispatchAndCheckContinue()) {
        return newResult(ExitStatus.NO_MOVE_PROGRESS, bytesLeftToMove, bytesBeingMoved);
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION);
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.INTERRUPTED);
    } finally {
      dispatcher.shutdownNow();
    }
  }

  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link Balancer} to work through all datanodes once.  
   * 开放给外部调用的run方法
   */
  static int run(Collection<URI> namenodes, final Parameters p,
      Configuration conf) throws IOException, InterruptedException {
    final long sleeptime =
        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);

    checkKeytabAndInit(conf);
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
    
    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes, 
            Balancer.class.getSimpleName(), BALANCER_ID_PATH, conf, p.maxIdleIteration);
    
      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
        	//初始化均衡器
          final Balancer b = new Balancer(nnc, p, conf);
          //均衡器执行balance操作
          final Result r = b.runOneIteration();
          r.print(iteration, System.out);

          // clean all lists
          b.resetData(conf);
          if (r.exitStatus == ExitStatus.IN_PROGRESS) {
            done = false;
          } else if (r.exitStatus != ExitStatus.SUCCESS) {
            //must be an error statue, return.
            return r.exitStatus.getExitCode();
          }
        }

        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        IOUtils.cleanup(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  private static void checkKeytabAndInit(Configuration conf)
      throws IOException {
    if (conf.getBoolean(DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_KEY,
        DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_DEFAULT)) {
      LOG.info("Keytab is configured, will login using keytab.");
      UserGroupInformation.setConfiguration(conf);
      String addr = conf.get(DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY,
          DFSConfigKeys.DFS_BALANCER_ADDRESS_DEFAULT);
      InetSocketAddress socAddr = NetUtils.createSocketAddr(addr, 0,
          DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY);
      SecurityUtil.login(conf, DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());
    }
  }

  /* *
   * Given elaspedTime in ms, return a printable string 
   *  elapse time:逝去的时光,均衡结束使用时间
   * */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  static class Parameters {
    static final Parameters DEFAULT = new Parameters(
        BalancingPolicy.Node.INSTANCE, 10.0,
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS,
        Collections.<String> emptySet(), Collections.<String> emptySet(),
        false);

    final BalancingPolicy policy;
    final double threshold;
    final int maxIdleIteration;
    // exclude the nodes in this set from balancing operations
    Set<String> nodesToBeExcluded;
    //include only these nodes in balancing operations
    Set<String> nodesToBeIncluded;
    /**
     * Whether to run the balancer during upgrade.
     */
    final boolean runDuringUpgrade;

    Parameters(BalancingPolicy policy, double threshold, int maxIdleIteration,
        Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded,
        boolean runDuringUpgrade) {
      this.policy = policy;
      this.threshold = threshold;
      this.maxIdleIteration = maxIdleIteration;
      this.nodesToBeExcluded = nodesToBeExcluded;
      this.nodesToBeIncluded = nodesToBeIncluded;
      this.runDuringUpgrade = runDuringUpgrade;
    }

    @Override
    public String toString() {
      return String.format("%s.%s [%s,"
              + " threshold = %s,"
              + " max idle iteration = %s, "
              + "number of nodes to be excluded = %s,"
              + " number of nodes to be included = %s,"
              + " run during upgrade = %s]",
          Balancer.class.getSimpleName(), getClass().getSimpleName(),
          policy, threshold, maxIdleIteration,
          nodesToBeExcluded.size(), nodesToBeIncluded.size(),
          runDuringUpgrade);
    }
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Balancer.
     * 
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
        return Balancer.run(namenodes, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Balancing took "
            + time2Str(Time.monotonicNow() - startTime));
      }
    }

    /**
     * parse command line arguments 
     * 解析命令行参数
     */
    static Parameters parse(String[] args) {
      BalancingPolicy policy = Parameters.DEFAULT.policy;
      double threshold = Parameters.DEFAULT.threshold;
      int maxIdleIteration = Parameters.DEFAULT.maxIdleIteration;
      Set<String> nodesTobeExcluded = Parameters.DEFAULT.nodesToBeExcluded;
      Set<String> nodesTobeIncluded = Parameters.DEFAULT.nodesToBeIncluded;
      boolean runDuringUpgrade = Parameters.DEFAULT.runDuringUpgrade;

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
            if ("-threshold".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Threshold value is missing: args = " + Arrays.toString(args));
              try {
                threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                    + args[i]);
                throw e;
              }
            } else if ("-policy".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Policy value is missing: args = " + Arrays.toString(args));
              try {
                policy = BalancingPolicy.parse(args[i]);
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal policy name: " + args[i]);
                throw e;
              }
            } else if ("-exclude".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "List of nodes to exclude | -f <filename> is missing: args = "
                  + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to exclude is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeExcluded = Util.getHostListFromFile(args[i], "exclude");
              } else {
                nodesTobeExcluded = Util.parseHostList(args[i]);
              }
            } else if ("-include".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "List of nodes to include | -f <filename> is missing: args = "
                + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to include is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeIncluded = Util.getHostListFromFile(args[i], "include");
               } else {
                nodesTobeIncluded = Util.parseHostList(args[i]);
              }
            } else if ("-idleiterations".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "idleiterations value is missing: args = " + Arrays
                      .toString(args));
              maxIdleIteration = Integer.parseInt(args[i]);
              LOG.info("Using a idleiterations of " + maxIdleIteration);
            } else if ("-runDuringUpgrade".equalsIgnoreCase(args[i])) {
              runDuringUpgrade = true;
              LOG.info("Will run the balancer even during an ongoing HDFS "
                  + "upgrade. Most users will not want to run the balancer "
                  + "during an upgrade since it will not affect used space "
                  + "on over-utilized machines.");
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
          checkArgument(nodesTobeExcluded.isEmpty() || nodesTobeIncluded.isEmpty(),
              "-exclude and -include options cannot be specified together.");
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      
      return new Parameters(policy, threshold, maxIdleIteration,
          nodesTobeExcluded, nodesTobeIncluded, runDuringUpgrade);
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  /**
   * Run a balancer
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting balancer due an exception", e);
      System.exit(-1);
    }
  }
}
