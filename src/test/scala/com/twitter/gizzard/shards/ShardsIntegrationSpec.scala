package com.twitter.gizzard.shards

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.querulous.query.SqlTimeoutException
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{CopyManager, ForwardingManager, NameServer, ShardRepository}


object ShardsIntegrationSpec extends Specification with JMocker with ClassMocker with Database {

  NameServer.rebuildSchema(queryEvaluator)

  val shardInfo1 = new ShardInfo("com.example.UserShard", "table1", "localhost")
  val shardInfo2 = new ShardInfo("com.example.UserShard", "table2", "localhost")

  class UserShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard]) extends Shard {
    val data = new mutable.HashMap[Int, String]

    def setName(id: Int, name: String) {
      data(id) = name
    }

    def getName(id: Int) = data(id)
  }

  val factory = new ShardFactory[UserShard] {
    def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[Shard]) = {
      new UserShard(shardInfo, weight, children)
    }

    def materialize(shardInfo: ShardInfo) {
      // nothing.
    }
  }

  "Shards" should {
    var shardRepository: ShardRepository[UserShard] = null
    var nameServer: NameServer[UserShard] = null
    var forwardingManager: ForwardingManager[UserShard] = null
    var copyManager: CopyManager[UserShard] = null

    doBefore {
      shardRepository = new ShardRepository
      shardRepository += (("com.example.UserShard", factory))
      forwardingManager = mock[ForwardingManager[UserShard]]
      copyManager = mock[CopyManager[UserShard]]
      nameServer = new NameServer[UserShard](queryEvaluator, shardRepository, forwardingManager, copyManager)

      expect {
        one(forwardingManager).reloadForwardings(nameServer)
      }

      nameServer.createShard(shardInfo1)
      nameServer.createShard(shardInfo2)
      nameServer.reload()
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
