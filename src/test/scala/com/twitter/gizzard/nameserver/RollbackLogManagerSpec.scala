package com.twitter.gizzard.nameserver

import java.nio.ByteBuffer
import scala.collection.generic.CanBuild

import com.twitter.gizzard.ConfiguredSpecification
import com.twitter.gizzard.shards.{NodeSet, RoutingNode}
import com.twitter.gizzard.thrift
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object RollbackLogManagerSpec extends ConfiguredSpecification {

  "RollbackLogManager" should {
    "implement topK for multiple sources" in {
      import RollbackLogManager.topK
      val (shared, second, third) = (logEntry(), logEntry(), logEntry())
      val inputs = Seq(
        Seq(),
        Seq(shared),
        Seq(shared, second),
        Seq(shared, third)
      )
      topK(inputs, 0) must beLike { case Seq() => true}
      topK(inputs, 1) must beLike { case Seq(shared) => true }
      topK(inputs, 2) must beLike { case Seq(shared, second) => true }
      topK(inputs, 3) must beLike { case Seq(shared, second, third) => true }
      topK(inputs, 4) must beLike { case Seq(shared, second, third) => true }
    }
  }
  
  val IdGen = new java.util.concurrent.atomic.AtomicInteger(0)
  def logEntry(
    id: Int = IdGen.getAndIncrement(),
    content: ByteBuffer = ByteBuffer.wrap(("" + IdGen.getAndIncrement).getBytes)
  ) = new thrift.LogEntry(id, content)
}
