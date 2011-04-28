package com.twitter.gizzard
package test

import shards.{RoutingNode, LeafRoutingNode, ShardInfo}

class FakeReadWriteShard[T](
  shard: T,
  shardInfo: ShardInfo,
  weight: Int,
  children: Seq[RoutingNode[T]])
extends LeafRoutingNode[T](shard, shardInfo, weight)
