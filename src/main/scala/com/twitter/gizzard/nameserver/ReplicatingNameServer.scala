package com.twitter.gizzard.nameserver

import shards._
import net.lag.logging.ThrottledLogger


class ReplicatingNameServer[S <: Shard](shardInfo: ShardInfo, weight: Int, replicas: Seq[Shard], log: ThrottledLogger[String], future: Future)
  extends ReplicatingShard[NameServer[S]](shardInfo, weight, replicas, log, future, None)
  with ReadWriteNameServer[S]
