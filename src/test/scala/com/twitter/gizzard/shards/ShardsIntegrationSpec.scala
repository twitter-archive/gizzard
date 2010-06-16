package com.twitter.gizzard.shards

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{IdGenerator, NameServer, SqlShard, ShardRepository}


object ShardsIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  val poolConfig = config.configMap("db.connection_pool")
  val shardInfo1 = new ShardInfo("com.example.UserShard", "table1", "localhost")
  val shardInfo2 = new ShardInfo("com.example.UserShard", "table2", "localhost")
  val queryEvaluator = evaluator(config.configMap("db"))
  materialize(config.configMap("db"))

  class UserShard(val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard]) extends Shard {
    val data = new mutable.HashMap[Int, String]

    def setName(id: Int, name: String) {
      data(id) = name
    }

    def getName(id: Int) = data(id)
  }

  val factory = new ShardFactory[UserShard] {
    def instantiate(nameServer: NameServer[UserShard], shardInfo: ShardInfo, weight: Int, children: Seq[UserShard]) = {
      new UserShard(shardInfo, weight, children)
    }

    def materialize(shardInfo: ShardInfo) {
      // nothing.
    }
  }

  "Shards" should {
    var shardRepository: ShardRepository[UserShard] = null
    var nameServerShard: nameserver.Shard = null
    var nameServer: NameServer[UserShard] = null

    var mapping = (a: Long) => a

    doBefore {
      shardRepository = new ShardRepository
      shardRepository += (("com.example.UserShard", factory))
      shardRepository += (("com.example.SqlShard", factory))
      reset(queryEvaluator)
      nameServerShard = new SqlShard(queryEvaluator)
      nameServer = new NameServer(nameServerShard, shardRepository, mapping)
      nameServer.reload()

      nameServer.createShard(shardInfo1)
      nameServer.createShard(shardInfo2)
    }

    "WriteOnlyShard" in {
      3 mustEqual 3
    }
  }
}
