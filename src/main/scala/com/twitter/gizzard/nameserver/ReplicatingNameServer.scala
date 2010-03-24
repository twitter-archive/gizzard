package com.twitter.gizzard.nameserver

import shards._
import net.lag.logging.ThrottledLogger


class ReplicatingNameServer(shardInfo: ShardInfo, weight: Int, replicas: Seq[ManagingNameServer], log: ThrottledLogger[String], future: Future)
  extends ReplicatingShard(shardInfo, weight, replicas, new LoadBalancer[ManagingNameServer](replicas), log, future, None)
  with ReadWriteNameServer
