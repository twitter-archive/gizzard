package com.twitter.gizzard.sharding


object Busy extends Enumeration {
  val Normal = Value("Normal")
  val Busy = Value("Busy")
}
