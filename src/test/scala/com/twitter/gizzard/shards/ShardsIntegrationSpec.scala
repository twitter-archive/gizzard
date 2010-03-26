package com.twitter.gizzard.shards

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluator
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{NameServer, SqlNameServerStore, NameServerStore, ShardRepository}


object ShardsIntegrationSpec extends Specification with JMocker with ClassMocker with Database {

//  SqlNameServer.rebuildSchema(queryEvaluator)

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
    def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[UserShard]) = {
      new UserShard(shardInfo, weight, children)
    }

    def materialize(shardInfo: ShardInfo) {
      // nothing.
    }
  }

  "Shards" should {
    var shardRepository: ShardRepository[UserShard] = null
    var nameServerStore: NameServerStore = null
    var nameServer: NameServer[UserShard] = null

    var mapping = (a: Long) => a

    doBefore {
      shardRepository = new ShardRepository
      shardRepository += (("com.example.UserShard", factory))
      shardRepository += (("com.example.SqlShard", factory))
      nameServerStore = new SqlNameServerStore(queryEvaluator)
      nameServer = new NameServer(nameServerStore, shardRepository, mapping)

      nameServer.createShard(shardInfo1)
      nameServer.createShard(shardInfo2)
//      nameServer.reload()
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
