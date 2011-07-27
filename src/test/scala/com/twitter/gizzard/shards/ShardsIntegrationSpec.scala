package com.twitter.gizzard.shards

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.gizzard.nameserver.{NameServer, SqlShardManagerSource}
import com.twitter.gizzard.ConfiguredSpecification


object ShardsIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  val shardInfo1     = new ShardInfo("com.example.UserShard", "table1", "localhost")
  val shardInfo2     = new ShardInfo("com.example.UserShard", "table2", "localhost")
  val queryEvaluator = evaluator(config)

  materialize(config)

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
    var nameServer: NameServer = null

    doBefore {
      reset(queryEvaluator)

      nameServer = new NameServer(
        LeafRoutingNode(new SqlShardManagerSource(queryEvaluator)),
        identity
      )

      val forwarder = nameServer.configureMultiForwarder[UserShard](
        _.shardFactories(
          "com.example.UserShard" -> factory,
          "com.example.SqlShard"  -> factory
        )
      )

      nameServer.reload()

      nameServer.shardManager.createAndMaterializeShard(shardInfo1)
      nameServer.shardManager.createAndMaterializeShard(shardInfo2)
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
