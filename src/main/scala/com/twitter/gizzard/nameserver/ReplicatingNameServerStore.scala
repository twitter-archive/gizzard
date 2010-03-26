package com.twitter.gizzard.nameserver

import shards._
import net.lag.logging.ThrottledLogger


class ReplicatingNameServerStore(replicas: Seq[NameServerStore], log: ThrottledLogger[String], future: Future)
  extends ReplicatingShard(
    new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingNameServerStore", "", ""),
    1, replicas, new LoadBalancer[NameServerStore](replicas), log, future, None)
  with ReadWriteNameServerStore
