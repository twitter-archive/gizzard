package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.querulous.config.Connection


sealed trait MappingFunction
object ByteSwapper extends MappingFunction
object Identity extends MappingFunction
object Fnv1a64 extends MappingFunction

sealed trait Replica
trait Mysql extends Replica with Connection
object Memory extends Replica

trait NameServer {
  val mappingFunction: MappingFunction
  val replicas: Seq[Replica]
  val writeTimeout: Duration
}
