package com.twitter.gizzard.nameserver

object HostStatus extends Enumeration {
  val Normal     = Value(0)
  val Blackholed = Value(1)
  val Blocked    = Value(2)
}

case class Host(hostname: String, port: Int, cluster: String, status: HostStatus.Value)
