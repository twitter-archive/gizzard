package com.twitter.gizzard.shards

import com.twitter.xrayspecs.Duration
import com.twitter.gizzard.nameserver.FailingOverLoadBalancer

import net.lag.configgy.ConfigMap

/*
 * The FailingOverLoadBalancer splits the shard list into online and offline shards.
 * Online shards are randomized according to the current weight-based formula. Offline
 * shards are randomly shuffled. To create the final list, 99% of the time, the first
 * online shard is first, followed by all offline shards, followed by the rest of the
 * online shards. 1% of the time, the offline shards are all first, to keep them warm.

 * This shard type helps with replication level 3 with one replica designated as a fallback.
 * The two online replicas will handle the majority of normal read traffic, but read
 * retries will fall over to the offline replica rather than the other online one.
 */


class FailingOverShardFactory[ConcreteShard <: Shard](
    readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard,
    future: Option[Future])
  extends ShardFactory[ConcreteShard] {

  def instantiate(info: shards.ShardInfo, weight: Int, replicas: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReplicatingShard(
      info,
      weight,
      replicas,
      new FailingOverLoadBalancer(replicas),
      future
    ))

  def materialize(shardInfo: shards.ShardInfo) = ()
}
