package com.twitter.gizzard.test

import shards.{ReadWriteShard, Shard, ShardInfo, FanoutResults}

class FakeReadWriteShard[S <: Shard](shard: S, val shardInfo: ShardInfo, val weight: Int, val children: Seq[S]) extends ReadWriteShard[S] {
  def readAllOperation[A](method: (S => A)) = FanoutResults(method, shard)
  def readOperation[A](method: (S => A)): A = method(shard)
  def writeOperation[A](method: (S => A)): A = method(shard)
  def rebuildableReadOperation[A](method: (S => Option[A]))(rebuild: (S, S) => Unit): Option[A] = method(shard)
}
