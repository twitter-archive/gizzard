package com.twitter.gizzard.shards

object FanoutResults {
  def apply[S, A](method: (S => A), shard: S): FanoutResults[A] = {
    try {
      val output = method(shard)
      FanoutResults(Seq(output), Seq[Throwable]())
    } catch {
      case e => FanoutResults(Seq[A](), Seq(e))
    }
  }
}

/**
 * It's both a Seq and a case class.  OMG Magic!
 */
case class FanoutResults[A](results: Seq[A], exceptions: Seq[Throwable]) extends SeqProxy[A] {
  val self = results
}