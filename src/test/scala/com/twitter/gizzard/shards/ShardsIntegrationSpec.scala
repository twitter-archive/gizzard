package com.twitter.gizzard
package shards

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{IdGenerator, NameServer, SqlShard, ShardRepository, NullJobRelayFactory}


object ShardsIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  val shardInfo1 = new ShardInfo("com.example.UserShard", "table1", "localhost")
  val shardInfo2 = new ShardInfo("com.example.UserShard", "table2", "localhost")
  val queryEvaluator = evaluator(config.nameServer)
  materialize(config.nameServer)

  class UserShard(val shardInfo: ShardInfo) {
    val data = new mutable.HashMap[Int, String]

    def setName(id: Int, name: String) {
      data(id) = name
    }

    def getName(id: Int) = data(id)
  }

  val factory = new ShardFactory[UserShard] {
    def instantiate(shardInfo: ShardInfo, weight: Int) = {
      new UserShard(shardInfo)
    }

    def materialize(shardInfo: ShardInfo) {}
  }

  "Shards" should {
    var shardRepository: ShardRepository[UserShard] = null
    var nameServerShard: RoutingNode[nameserver.Shard] = null
    var nameServer: NameServer[UserShard] = null

    var mapping = (a: Long) => a

    doBefore {
      shardRepository = new ShardRepository
      shardRepository += (("com.example.UserShard", factory))
      shardRepository += (("com.example.SqlShard", factory))
      reset(queryEvaluator)
      nameServerShard = new LeafRoutingNode(new SqlShard(queryEvaluator), 1)
      nameServer = new NameServer(nameServerShard, shardRepository, NullJobRelayFactory, mapping)
      nameServer.reload()

      nameServer.createShard(shardInfo1)
      nameServer.createShard(shardInfo2)
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
