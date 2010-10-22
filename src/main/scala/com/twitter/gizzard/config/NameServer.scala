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
  def queryEvaluator: QueryEvaluator

  def mappingFunction: MappingFunction
  def replicas: Seq[Replica]
  def writeTimeout: Duration
}
