package com.twitter.gizzard.shards

import java.sql.SQLException
import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.{LoadBalancer, NameServer}
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.{Logger, ThrottledLogger}


class SplittingShardFactory[ConcreteShard <: Shard](
      readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard) 
      extends shards.ShardFactory[ConcreteShard] {

  def instantiate(nameServer: NameServer[ConcreteShard], shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[ConcreteShard]) =
    readWriteShardAdapter(new SplittingShard(nameServer, shardInfo, weight, replicas))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class SplittingShard[ConcreteShard <: Shard](
      val nameServer: nameserver.Shard[ConcreteShard], val shardInfo: ShardInfo, val weight: Int, val children: Seq[ConcreteShard]) 
      extends ReadWriteShard[ConcreteShard] {

  override def readOperation[A](address: (Int, Long), method: (ConcreteShard => A))  = method(nameServer.findCurrentForwarding(address))
  override def writeOperation[A](address: (Int, Long), method: (ConcreteShard => A)) = method(nameServer.findCurrentForwarding(address))
}
