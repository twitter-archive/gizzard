package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.querulous.evaluator

import com.twitter.gizzard
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards


trait MappingFunction extends (() => Long => Long)

object ByteSwapper extends MappingFunction { def apply() = nameserver.ByteSwapper }
object Identity extends MappingFunction { def apply() = identity _ }
object Fnv1a64 extends MappingFunction { def apply() = nameserver.FnvHasher }
object Hash extends MappingFunction { def apply() = nameserver.FnvHasher }

trait Replica

trait Mysql extends Replica {
  def connection: Connection
  var queryEvaluator: QueryEvaluator = new QueryEvaluator

  lazy val builtEvaluator = queryEvaluator()
  def apply[T](builder: evaluator.QueryEvaluator => T) = builder(builtEvaluator(connection))
}

object Memory extends Replica
