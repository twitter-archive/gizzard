package com.twitter.gizzard.nameserver

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.gizzard.ConfiguredSpecification
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import java.util.concurrent.TimeUnit

class PollingUpdaterSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  "PollingUpdater" should {
    "Start, poll, and stop" in {
      val shard = mock[ShardManagerSource]
      val nameServer = new NameServer(LeafRoutingNode(shard), identity)
      expect {
        atLeast(1).of(shard).getCurrentStateVersion() willReturn (0L)
        atLeast(1).of(shard).getMasterStateVersion() willReturn (0L)
      }
      val pollingUpdater = new PollingUpdater(nameServer, 1.second)
      pollingUpdater.start()
      Thread.sleep(200)
      pollingUpdater.stop()
    }

    "Reload on version change" in {
      val shard = mock[ShardManagerSource]
      val nameServer = new NameServer(LeafRoutingNode(shard), identity)
      expect {
        exactly(4).of(shard).getCurrentStateVersion() willReturnEach (0L, 0L, 2L, 2L)
        exactly(4).of(shard).getMasterStateVersion() willReturnEach (0L, 2L, 2L, 5L)
        exactly(2).of(shard).reload
        exactly(2).of(shard).currentState willReturn (List[NameServerState]())
      }

      val pollingUpdater = new PollingUpdater(nameServer, 1.second)
      pollingUpdater.poll()
      pollingUpdater.poll()
      pollingUpdater.poll()
      pollingUpdater.poll()
    }

    "Not reload when there is no version change" in {
      val shard = mock[ShardManagerSource]
      val nameServer = new NameServer(LeafRoutingNode(shard), identity)
      expect {
        exactly(1).of(shard).getCurrentStateVersion() willReturnEach (10L)
        exactly(1).of(shard).getMasterStateVersion() willReturnEach (10L)
      }

      val pollingUpdater = new PollingUpdater(nameServer, 1.second)
      pollingUpdater.poll()
    }
  }
}
