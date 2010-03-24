package com.twitter.gizzard.nameserver

import shards._
import net.lag.logging.ThrottledLogger


class ReplicatingNameServer(replicas: Seq[ManagingNameServer], log: ThrottledLogger[String], future: Future)
  extends ReplicatingShard(
    new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingNameServer", "", ""),
    1, replicas, new LoadBalancer[ManagingNameServer](replicas), log, future, None)
  with ReadWriteNameServer
