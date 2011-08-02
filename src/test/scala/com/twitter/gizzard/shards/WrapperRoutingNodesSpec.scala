package com.twitter.gizzard.shards

import com.twitter.conversions.time._
import org.specs.Specification
import com.twitter.util.{Try, Return, Throw, Future}


object WrapperRoutingNodesSpec extends Specification {
  case class Fake(i: Int, readOnly: Boolean)

  val leafs = (0 to 1) map { i =>
    LeafRoutingNode(Fake(i, true), Fake(i, false), new ShardInfo("", i.toString, ""), 1)
  }

  "BlockedShard" in {
    val blocked = BlockedShard(new ShardInfo("", "blocked", ""), 1, Seq(leafs(0)))

    blocked.read.activeShards.size   mustEqual 0
    blocked.read.blockedShards.size  mustEqual 1
    blocked.write.activeShards.size  mustEqual 0
    blocked.write.blockedShards.size mustEqual 1

    (blocked.read all { _.i } head)  must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
    (blocked.write all { _.i } head) must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }

    val emptyBlocked = BlockedShard[Fake](new ShardInfo("", "blocked", ""), 1, Seq())

    emptyBlocked.read.activeShards.size   mustEqual 0
    emptyBlocked.read.blockedShards.size  mustEqual 1
    emptyBlocked.write.activeShards.size  mustEqual 0
    emptyBlocked.write.blockedShards.size mustEqual 1

    (emptyBlocked.read all { _.i } head)  must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
    (emptyBlocked.write all { _.i } head) must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
  }

  "BlackHoleShard" in {
    val blackhole = BlackHoleShard(new ShardInfo("", "blackhole", ""), 1, Seq(leafs(0)))

    blackhole.read.activeShards.size   mustEqual 0
    blackhole.read.blockedShards.size  mustEqual 0
    blackhole.write.activeShards.size  mustEqual 0
    blackhole.write.blockedShards.size mustEqual 0

    (blackhole.read all { _.i } size)  mustEqual 0
    (blackhole.write all { _.i } size) mustEqual 0

    val emptyBlackhole = BlackHoleShard[Fake](new ShardInfo("", "blackhole", ""), 1, Seq())

    emptyBlackhole.read.activeShards.size   mustEqual 0
    emptyBlackhole.read.blockedShards.size  mustEqual 0
    emptyBlackhole.write.activeShards.size  mustEqual 0
    emptyBlackhole.write.blockedShards.size mustEqual 0

    (emptyBlackhole.read  all { _.i } size) mustEqual 0
    (emptyBlackhole.write all { _.i } size) mustEqual 0
  }

  "WriteOnlyShard" in {
    val writeOnly = WriteOnlyShard(new ShardInfo("", "writeonly", ""), 1, Seq(leafs(0)))

    writeOnly.read.activeShards.size   mustEqual 0
    writeOnly.read.blockedShards.size  mustEqual 1
    writeOnly.write.activeShards.size  mustEqual 1
    writeOnly.write.blockedShards.size mustEqual 0

    (writeOnly.read all { _.i } head)  must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
    (writeOnly.write all { _.i } head) must beLike { case Return(_) => true }
  }

  "ReadOnlyShard" in {
    val readOnly = ReadOnlyShard(new ShardInfo("", "readonly", ""), 1, Seq(leafs(0)))

    readOnly.read.activeShards.size   mustEqual 1
    readOnly.read.blockedShards.size  mustEqual 0
    readOnly.write.activeShards.size  mustEqual 0
    readOnly.write.blockedShards.size mustEqual 1

    (readOnly.read all { _.i } head)  must beLike { case Return(_) => true }
    (readOnly.write all { _.i } head) must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
  }

  "SlaveShard" in {
    val slave = SlaveShard(new ShardInfo("", "slave", ""), 1, Seq(leafs(0)))

    slave.read.activeShards.size   mustEqual 1
    slave.read.blockedShards.size  mustEqual 0
    slave.write.activeShards.size  mustEqual 0
    slave.write.blockedShards.size mustEqual 0

    (slave.read all { _.i } head)  must beLike { case Return(_) => true }
    (slave.write all { _.i } size) mustEqual 0
  }
}
