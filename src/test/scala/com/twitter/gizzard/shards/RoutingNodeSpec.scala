package com.twitter.gizzard.shards

import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.util.{Return, Throw}


object RoutingNodeSpec extends Specification {
  case class Fake(i: Int, readOnly: Boolean)

  val leafs  = (0 to 5) map { i => LeafRoutingNode(Fake(i, true), Fake(i, false), new ShardInfo("", i.toString, ""), Weight.Default) }

  "RoutingNode" in {
    val normal    = leafs(0)
    val readOnly  = ReadOnlyShard(new ShardInfo("","ro",""), Weight.Default, Seq(leafs(1)))
    val writeOnly = WriteOnlyShard(new ShardInfo("","wo",""), Weight.Default, Seq(leafs(2)))
    val blocked   = BlockedShard(new ShardInfo("","","b"), Weight.Default, Seq(leafs(3)))
    val blackHole = BlackHoleShard(new ShardInfo("","bh",""), Weight.Default, Seq(leafs(4)))
    val slave     = SlaveShard(new ShardInfo("","s",""), Weight.Default, Seq(leafs(5)))

    val node = ReplicatingShard(new ShardInfo("","",""), Weight.Default, Seq(normal, readOnly, writeOnly, blocked, blackHole, slave))

    "read" in {
      (node.read.activeShards map { _._1.tablePrefix } sorted) must haveTheSameElementsAs(Seq("0", "1", "5"))
      (node.read.blockedShards map { _.tablePrefix } sorted)   must haveTheSameElementsAs(Seq("2", "3"))

      // pull out the fake shards themselve
      val shards = node.read all { identity(_) } collect { case Return(t) => t }

      // should all be readonly on read
      for (s <- shards) s.readOnly mustEqual true
    }

    "write" in {
      (node.write.activeShards map { _._1.tablePrefix } sorted) must haveTheSameElementsAs(Seq("0", "2"))
      (node.write.blockedShards map { _.tablePrefix } sorted)   must haveTheSameElementsAs(Seq("1", "3"))

      // pull out the fake shards themselve
      val shards = node.write all { identity(_) } collect { case Return(t) => t }

      // should never be readonly on write
      for (s <- shards) s.readOnly mustEqual false
    }

    "equals" in {}
    "hashCode" in {}

    "deprecated methods (tested in ReplicatingShardSpec)" in {
      "readAllOperation" in {}
      "readOperation" in {}
      "writeOperation" in {}
      "rebuildableReadOperation" in {}
    }
  }
}
