package com.twitter.gizzard.fake


class Job(protected val attributes: Map[String, Any]) extends jobs.UnboundJob[Int] {
  def toMap = attributes

  override def equals(that: Any) = that match {
    case that: Job => attributes == that.attributes
    case _ => false
  }

  def apply(i: Int) = ()
}
