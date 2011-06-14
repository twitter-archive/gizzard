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
  val shardInfo1     = new ShardInfo("com.example.UserShard", "table1", "localhost")
  val shardInfo2     = new ShardInfo("com.example.UserShard", "table2", "localhost")
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
    def instantiate(shardInfo: ShardInfo, weight: Int)         = new UserShard(shardInfo)
    def instantiateReadOnly(shardInfo: ShardInfo, weight: Int) = new UserShard(shardInfo)
    def materialize(shardInfo: ShardInfo) {}
  }

  "Shards" should {
    var shardRepository: ShardRepository               = null
    var nameServerShard: RoutingNode[nameserver.Shard] = null
    var nameServer: NameServer                         = null

    var mapping = (a: Long) => a

    doBefore {
      reset(queryEvaluator)
      nameServerShard = LeafRoutingNode(new SqlShard(queryEvaluator))
      nameServer      = new NameServer(nameServerShard, NullJobRelayFactory, mapping)

      val forwarder = nameServer.configureMultiForwarder[UserShard](
        _.shardFactories(
          "com.example.UserShard" -> factory,
          "com.example.SqlShard"  -> factory
        )
      )

      nameServer.reload()

      nameServer.createAndMaterializeShard(shardInfo1)
      nameServer.createAndMaterializeShard(shardInfo2)
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
