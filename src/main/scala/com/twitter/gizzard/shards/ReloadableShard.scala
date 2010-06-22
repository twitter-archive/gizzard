package com.twitter.gizzard.shards

import java.sql.SQLException
import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.{Logger, ThrottledLogger}

class ReloadableShardFactory[ConcreteShard <: Shard](
      readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard,
      treeFactory: ShardTreeFactory[ConcreteShard]) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[ConcreteShard]) = readWriteShardAdapter(new ReloadableShard(shardInfo, weight, children, treeFactory))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReloadableShard[ConcreteShard <: Shard](
    val shardInfo: ShardInfo, val weight: Int, var children: Seq[ConcreteShard], val factory: ShardTreeFactory[ConcreteShard])
    extends ReadWriteShard[ConcreteShard] {
  
  def reload = {
    children = Seq(factory.instantiate(children.first.shardInfo.id))
  }
      
  def readOperation[A](address: Option[Address], method: (ConcreteShard => A)) = method(children.first)
  def writeOperation[A](address: Option[Address], method: (ConcreteShard => A)) = method(children.first)
}
