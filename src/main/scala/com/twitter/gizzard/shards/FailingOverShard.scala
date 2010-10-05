package com.twitter.gizzard.shards

import com.twitter.xrayspecs.Duration
import com.twitter.gizzard.nameserver.FailingOverLoadBalancer

class FailingOverShardFactory[ConcreteShard <: Shard](
    readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard,
    future: Option[Future],
    timeout: Duration)
  extends ShardFactory[ConcreteShard] {

  def instantiate(info: shards.ShardInfo, weight: Int, replicas: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReplicatingShard(
      info,
      weight,
      replicas,
      new FailingOverLoadBalancer(replicas),
      future,
      timeout
    ))

  def materialize(shardInfo: shards.ShardInfo) = ()
}
