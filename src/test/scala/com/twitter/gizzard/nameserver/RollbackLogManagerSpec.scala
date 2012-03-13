package com.twitter.gizzard.nameserver

import java.nio.ByteBuffer

import com.twitter.gizzard.shards._
import com.twitter.gizzard.ConfiguredSpecification
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class RollbackLogManagerSpec extends ConfiguredSpecification with JMocker with ClassMocker {

  "RollbackLogManager" should {
    "implement peek for multiple sources" in {
      val shard = mock[RoutingNode[ShardManagerSource]]
      val rlm = new RollbackLogManager(shard)
      val logId = ByteBuffer.wrap("eyed".getBytes())
      rlm.entryPeek(logId, 2)
    }
  }
}
