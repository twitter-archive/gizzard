package com.twitter.gizzard.shards


object Busy extends Enumeration {
  val Normal     = Value("Normal")
  val Busy       = Value("Busy")
  val Error      = Value("Error")
  val Cancelled  = Value("Cancelled")
}
