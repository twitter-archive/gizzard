package com.twitter.gizzard.sharding

import com.twitter.ostrich.W3CReporter
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{Logger, ThrottledLogger}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object ReplicatingShardSpec extends Specification with JMocker with ClassMocker {
  abstract class FakeShard extends Shard {
    // the 'throws' clauses are necessary for mocks to work.
    @throws(classOf[ShardException]) def getName(): String
    @throws(classOf[ShardException]) def setName(name: String)
  }

  class FakeReplicatingShard(shardInfo: ShardInfo, weight: Int, replicas: Seq[FakeShard],
                             log: ThrottledLogger[String], future: Future,
                             eventLogger: Option[W3CReporter])
    extends ReplicatingShard[FakeShard](shardInfo, weight, replicas, log, future, eventLogger) {

    def getName() = readOperation(_.getName)
    def setName(name: String) = writeOperation(_.setName(name))
  }


  "ReplicatingShard" should {
    var shard1: FakeShard = null
    var shard2: FakeShard = null
    var shard3: FakeShard = null
    var replicatingShard: FakeReplicatingShard = null
    val future = new Future("Future!", 1, 1, 1.second)
    val log = new ThrottledLogger[String](Logger(), 1, 1)

    doBefore {
      shard1 = mock[FakeShard]
      shard2 = mock[FakeShard]
      shard3 = mock[FakeShard]
      replicatingShard = new FakeReplicatingShard(null, 1, List(shard1, shard2), log, future, None)
    }

    "weights are honored" in {
      expect {
        allowing(shard1).weight willReturn 1
        allowing(shard2).weight willReturn 2
        allowing(shard3).weight willReturn 1
      }
      val shards = List(shard1, shard2, shard3)
      replicatingShard.getNext(0.0f, shards) mustEqual (shard1, List(shard2, shard3))
      replicatingShard.getNext(0.24f, shards) mustEqual (shard1, List(shard2, shard3))
      replicatingShard.getNext(0.4f, shards) mustEqual (shard2, List(shard1, shard3))
      replicatingShard.getNext(0.6f, shards) mustEqual (shard2, List(shard1, shard3))
      replicatingShard.getNext(0.74f, shards) mustEqual (shard2, List(shard1, shard3))
      replicatingShard.getNext(0.75f, shards) mustEqual (shard3, List(shard1, shard2))
      replicatingShard.getNext(0.9f, shards) mustEqual (shard3, List(shard1, shard2))
      replicatingShard.getNext(1.0f, shards) mustEqual (shard3, List(shard1, shard2))
    }

    "failover" in {
      "when shard1 throws an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          allowing(shard1).weight willReturn 1
          allowing(shard2).weight willReturn 0
          one(shard1).shardInfo willReturn shard1Info
          one(shard1).getName().willThrow(exception) then
          one(shard2).getName().willReturn("bob")
        }
        replicatingShard.getName() mustEqual "bob"
      }

      "when all shards throw an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          allowing(shard1).weight willReturn 1
          allowing(shard2).weight willReturn 0
          one(shard1).shardInfo willReturn shard1Info
          one(shard2).shardInfo willReturn shard1Info
          one(shard1).getName().willThrow(exception) then
          one(shard2).getName().willThrow(exception)
        }
        replicatingShard.getName() must throwA[ShardException]
      }
    }

    "writes happen to all shards" in {
      "when they succeed" in {
        expect {
          allowing(shard1).weight willReturn 1
          allowing(shard2).weight willReturn 0
          one(shard1).setName("alice")
          one(shard2).setName("alice")
        }
        replicatingShard.setName("alice")
      }

      "when the first one fails" in {
        expect {
          allowing(shard1).weight willReturn 1
          allowing(shard2).weight willReturn 0
          one(shard1).setName("alice").willThrow(new ShardException("o noes"))
          one(shard2).setName("alice")
        }
        replicatingShard.setName("alice") must throwA[Exception]
      }
    }

    "reads happen to shards according to weight" in {
      "zero percent" in {
        expect {
          allowing(shard1).weight willReturn 1
          allowing(shard2).weight willReturn 0
          one(shard1).getName().willReturn("ted")
        }
        replicatingShard.getName() mustEqual "ted"
      }

      "one hundred percent" in {
        expect {
          allowing(shard1).weight willReturn 0
          allowing(shard2).weight willReturn 1
          one(shard2).getName().willReturn("ted")
        }
        replicatingShard.getName() mustEqual "ted"
      }
    }
  }
}
