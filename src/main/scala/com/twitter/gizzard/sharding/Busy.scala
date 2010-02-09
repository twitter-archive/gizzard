package com.twitter.gizzard.sharding


object Busy extends Enumeration {
  val Normal = Value("Normal")
  val Busy = Value("Busy")

  def fromThrift(busy: Int) = busy match {
    case 1 => Busy
    case _ => Normal
  }

  def toThrift(busy: Value) = busy match {
    case Normal => 0
    case Busy => 1
  }
}
