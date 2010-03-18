package com.twitter.gizzard.nameserver

import shards._
import net.lag.logging.ThrottledLogger


class ReplicatingNameServer(shardInfo: ShardInfo, weight: Int, replicas: Seq[Shard], log: ThrottledLogger[String], future: Future)
  extends ReplicatingShard[ManagingNameServer](shardInfo, weight, replicas, log, future, None)
  with ReadWriteNameServer
