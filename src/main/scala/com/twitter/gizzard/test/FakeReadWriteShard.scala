package com.twitter.gizzard
package test

import shards.{RoutingNode, LeafRoutingNode, ShardInfo}

class FakeReadWriteShard[T](
  shard: T,
  shardInfo: ShardInfo,
  weight: Int,
  override val children: Seq[RoutingNode[T]])
extends LeafRoutingNode[T](shardInfo, weight, shard)
