package com.twitter.gizzard.shards

import java.util.concurrent.CancellationException
import com.twitter.conversions.time._
import org.specs.Specification
import com.twitter.util.{Try, Return, Throw, Future, Promise}


object NodeSetSpec extends Specification {
  case class Fake(i: Int, readOnly: Boolean)

  val leafs = (0 to 8) map { i =>
    LeafRoutingNode(Fake(i, true), Fake(i, false), new ShardInfo("", i.toString, ""), 1)
  }

  val normals   = leafs take 3
  val blocked   = leafs drop 3 take 3 map { l =>  BlockedShard(new ShardInfo("","b",""), 1, Seq(l)) }
  val blackhole = leafs drop 6 take 3 map { l =>  BlackHoleShard(new ShardInfo("","b",""), 1, Seq(l)) }

  val allNormal    = ReplicatingShard(new ShardInfo("","",""), 1, normals)
  val noNormal     = ReplicatingShard(new ShardInfo("","",""), 1, blocked ++ blackhole)
  val allBlocked   = ReplicatingShard(new ShardInfo("","",""), 1, blocked)
  val noBlocked    = ReplicatingShard(new ShardInfo("","",""), 1, normals ++ blackhole)
  val allBlackhole = ReplicatingShard(new ShardInfo("","",""), 1, blackhole)
  val noBlackhole  = ReplicatingShard(new ShardInfo("","",""), 1, normals ++ blocked)
  val mixed        = ReplicatingShard(new ShardInfo("","",""), 1, normals ++ blocked ++ blackhole)

  "NodeIterable" in {
    "activeShards" in {
      (allNormal.read.activeShards map { _._2.i } sorted)   must haveTheSameElementsAs(Seq(0, 1, 2))
      (noBlocked.read.activeShards map { _._2.i } sorted)   must haveTheSameElementsAs(Seq(0, 1, 2))
      (noBlackhole.read.activeShards map { _._2.i } sorted) must haveTheSameElementsAs(Seq(0, 1, 2))
      (mixed.read.activeShards map { _._2.i } sorted)       must haveTheSameElementsAs(Seq(0, 1, 2))

      (noNormal.read.activeShards map { _._2.i } sorted)     must haveTheSameElementsAs(Seq())
      (allBlocked.read.activeShards map { _._2.i } sorted)   must haveTheSameElementsAs(Seq())
      (allBlackhole.read.activeShards map { _._2.i } sorted) must haveTheSameElementsAs(Seq())
    }

    "lookupId" in {
      allNormal.read foreach { s => allNormal.read.lookupId(s).get.tablePrefix mustEqual s.i.toString }
    }

    "blockedShards" in {
      (noNormal.read.blockedShards map { _.tablePrefix } sorted)    must haveTheSameElementsAs(Seq("3", "4", "5"))
      (allBlocked.read.blockedShards map { _.tablePrefix } sorted)  must haveTheSameElementsAs(Seq("3", "4", "5"))
      (noBlackhole.read.blockedShards map { _.tablePrefix } sorted) must haveTheSameElementsAs(Seq("3", "4", "5"))
      (mixed.read.blockedShards map { _.tablePrefix } sorted)       must haveTheSameElementsAs(Seq("3", "4", "5"))

      (allNormal.read.blockedShards map { _.tablePrefix } sorted)    must haveTheSameElementsAs(Seq())
      (noBlocked.read.blockedShards map { _.tablePrefix } sorted)    must haveTheSameElementsAs(Seq())
      (allBlackhole.read.blockedShards map { _.tablePrefix } sorted) must haveTheSameElementsAs(Seq())
    }

    "containsBlocked" in {
      noNormal.read.containsBlocked     mustEqual true
      allBlocked.read.containsBlocked   mustEqual true
      noBlackhole.read.containsBlocked  mustEqual true
      mixed.read.containsBlocked        mustEqual true

      allNormal.read.containsBlocked    mustEqual false
      noBlocked.read.containsBlocked    mustEqual false
      allBlackhole.read.containsBlocked mustEqual false
    }

    "anyOption" in {
      allNormal.read.anyOption { _.i }                must beLike { case Some(_) => true }
      allNormal.read.anyOption { _ => error("oops") } must throwA[Exception]

      allBlocked.read.anyOption { _.i }   must beLike { case None => true }
      allBlackhole.read.anyOption { _.i } must beLike { case None => true }

      mixed.read.anyOption { _.i } must beLike { case Some(_) => true }
    }

    "tryAny" in {
      allNormal.read.tryAny { _ => Return(true) }                              must beLike { case Return(_) => true }
      allNormal.read.tryAny { _ => Try(error("oops")) }                        must beLike { case Throw(_)  => true }
      allNormal.read.tryAny { s => Try(if (s.i == 0) s.i else error("oops")) } must beLike { case Return(0) => true }

      allBlocked.read.tryAny { _ => Return(true) }   must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] }
      allBlackhole.read.tryAny { _ => Return(true) } must beLike { case Throw(e) => e.isInstanceOf[ShardBlackHoleException] }

      mixed.read.tryAny { _ => Return(true) } must beLike { case Return(_) => true }
    }

    "futureAny" in {
      (allNormal.read.futureAny { _ => Future(true) } apply())                                 mustEqual true
      (allNormal.read.futureAny { _ => Future[Any](error("oops")) } apply())                   must throwA[Exception]
      (allNormal.read.futureAny { s => Future(if (s.i == 0) s.i else error("oops")) } apply()) mustEqual 0
     // (allNormal.read.futureAny { s => Future(if (s.i == 0) s.i else throw new CancellationException) } apply()) must throwA[Exception]

      (allBlocked.read.futureAny { _ => Future(true) } apply())   must throwA[ShardOfflineException]
      (allBlackhole.read.futureAny { _ => Future(true) } apply()) must throwA[ShardBlackHoleException]

      (mixed.read.futureAny { _ => Future(true) } apply()) mustEqual true

      val p = new Promise[Unit]
      p onCancellation { p.update(Throw(new CancellationException)) }
      val f = allNormal.read.futureAny { s => p }
      f cancel()
      (f apply()) must throwA[Exception]
    }

    "any" in {
      allNormal.read.any { _ => true }                mustEqual true
      allNormal.read.any { _ => error("oops"); true } must throwA[Exception]

      allBlocked.read.any { _ => true }   must throwA[ShardOfflineException]
      allBlackhole.read.any { _ => true } must throwA[ShardBlackHoleException]

      mixed.read.any { _ => true } mustEqual true
    }

    "fmap" in {
      val normalRV = allNormal.read.fmap { _ => Future(true) }
      normalRV.size mustEqual 3
      normalRV foreach { _ respond { _ must beLike { case Return(_) => true } } }

      val normalThrowed = allNormal.read.fmap { _ => Future { error("oops"); true } }
      normalThrowed.size mustEqual 3
      normalThrowed foreach { _ respond { _ must beLike { case Throw(_) => true } } }

      val blockedRV = allBlocked.read.fmap { _ => Future(true) }
      blockedRV.size mustEqual 3
      blockedRV foreach { _ respond { _ must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] } } }

      val blackholeRV = allBlackhole.read.fmap { _ => Future(true) }
      blackholeRV.size mustEqual 0

      val mixedRV = mixed.read.fmap { _ => Future(true) }
      mixedRV.size mustEqual 6
      mixedRV foreach { _ respond { _ must beLike {
        case Return(_) => true
        case Throw(e)  => e.isInstanceOf[ShardOfflineException]
      } } }
    }

    "all" in {
      val normalRV = allNormal.read.all { _ => true }
      normalRV.size mustEqual 3
      normalRV foreach { _ must beLike { case Return(_) => true } }

      val normalThrowed = allNormal.read.all { _ => error("oops"); true }
      normalThrowed.size mustEqual 3
      normalThrowed foreach { _ must beLike { case Throw(_) => true } }

      val blockedRV = allBlocked.read.all { _ => true }
      blockedRV.size mustEqual 3
      blockedRV foreach { _ must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] } }

      val blackholeRV = allBlackhole.read.all { _ => true }
      blackholeRV.size mustEqual 0

      val mixedRV = mixed.read.all { _ => true }
      mixedRV.size mustEqual 6
      mixedRV foreach { _ must beLike {
        case Return(_) => true
        case Throw(e)  => e.isInstanceOf[ShardOfflineException]
      } }
    }

    "tryAll" in {
      val normalRV = allNormal.read.tryAll { _ => Return(true) }
      normalRV.size mustEqual 3
      normalRV foreach { _ must beLike { case Return(_) => true } }

      val normalThrowed = allNormal.read.tryAll { _ => Try { error("oops"); true } }
      normalThrowed.size mustEqual 3
      normalThrowed foreach { _ must beLike { case Throw(_) => true } }

      val blockedRV = allBlocked.read.tryAll { _ => Return(true) }
      blockedRV.size mustEqual 3
      blockedRV foreach { _ must beLike { case Throw(e) => e.isInstanceOf[ShardOfflineException] } }

      val blackholeRV = allBlackhole.read.tryAll { _ => Return(true) }
      blackholeRV.size mustEqual 0

      val mixedRV = mixed.read.tryAll { _ => Return(true) }
      mixedRV.size mustEqual 6
      mixedRV foreach { _ must beLike {
        case Return(_) => true
        case Throw(e)  => e.isInstanceOf[ShardOfflineException]
      } }
    }

    "iterator" in {}

    "map" in {
      allNormal.read.map { _ => true } must haveTheSameElementsAs(Seq(true, true, true))
      noBlocked.read.map { _ => true } must haveTheSameElementsAs(Seq(true, true, true))

      allNormal.read.map { _ => error("oops"); true } must throwA[Exception]

      allBlackhole.read.map { _ => true } must haveTheSameElementsAs(Seq())

      allBlocked.read.map { _ => true } must throwA[ShardOfflineException]
      mixed.read.map { _ => true }      must throwA[ShardOfflineException]
    }

    "flatMap" in {
      allNormal.read.flatMap { _ => Seq(true) }  must haveTheSameElementsAs(Seq(true, true, true))
      noBlocked.read.flatMap { _ => Seq(true) }  must haveTheSameElementsAs(Seq(true, true, true))
      allNormal.read.flatMap { _ => Seq[Int]() } must haveTheSameElementsAs(Seq())

      allNormal.read.flatMap { _ => error("oops"); Seq(true) } must throwA[Exception]

      allBlackhole.read.flatMap { _ => Seq(true) } must haveTheSameElementsAs(Seq())

      allBlocked.read.flatMap { _ => Seq(true) } must throwA[ShardOfflineException]
      mixed.read.flatMap { _ => Seq(true) }      must throwA[ShardOfflineException]
    }

    "foreach" in {
      allNormal.read.foreach { _ => true }    must not(throwA[Exception])
      allBlackhole.read.foreach { _ => true } must not(throwA[Exception])

      allBlocked.read.foreach { _ => true } must throwA[ShardOfflineException]
      mixed.read.foreach { _ => true }      must throwA[ShardOfflineException]
    }
  }

  "NodeSet" in {
    "par XXX: better test of parallel execution and config inheritance?" in {
      allNormal.read.par must haveClass[ParNodeSet[Fake]]
      allNormal.read.par must haveSuperClass[NodeSet[Fake]]
    }

    "filter" in {
      (allNormal.read.filter { (info, opt) => info.tablePrefix == "0" } size) mustEqual 1
      (mixed.read.filter { (info, opt) => opt.isEmpty } size) mustEqual 3
    }

    "filterNot" in {
      (allNormal.read.filterNot { (info, opt) => info.tablePrefix == "0" } size) mustEqual 2
    }

    "skip" in {
      (mixed.read.skip(ShardId("", "0")).activeShards map { _._1.tablePrefix } sorted) must haveTheSameElementsAs(Seq("1", "2"))
      (mixed.read.skip(ShardId("", "4")).blockedShards map { _.tablePrefix } sorted)   must haveTheSameElementsAs(Seq("3", "5"))
    }
  }
}
