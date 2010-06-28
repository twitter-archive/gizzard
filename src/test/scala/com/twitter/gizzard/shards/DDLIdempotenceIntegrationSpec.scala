package com.twitter.gizzard.shards

import net.lag.logging.{ThrottledLogger, Logger}
import java.sql.SQLException
import com.twitter.xrayspecs.TimeConversions._
import scala.collection.mutable
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{Forwarding, IdGenerator, SqlShard, ShardRepository, BasicShardRepository}

object DDLIdempotenceIntegrationSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  val poolConfig = config.configMap("db.connection_pool")
  val queryEvaluator = evaluator(config.configMap("db"))
  materialize(config.configMap("db"))
  val adapter = (shard:shards.ReadWriteShard[fake.Shard]) => new fake.ReadWriteShardAdapter(shard)
  val future = new Future("Future!", 1, 1, 1.second, 1.second)
  val log = new ThrottledLogger[String](Logger(), 1, 1)
    
  val repo = new BasicShardRepository[fake.Shard](adapter, log, future)
  repo += ("com.twitter.gizzard.fake.NestableShard" -> new fake.NestableShardFactory())
  reset(queryEvaluator)
  val nameServerShard = new SqlShard(queryEvaluator)
  
  "Idempotence" should {
    "be creatable" in {
      val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
      nameServerShard.createShard(shardInfo, repo)
      nameServerShard.createShard(shardInfo, repo)
      nameServerShard.getShard(shardInfo.id) mustEqual shardInfo
    }
    "be deletable" in {
      val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
      nameServerShard.createShard(shardInfo, repo)
      nameServerShard.deleteShard(shardInfo.id)
      nameServerShard.deleteShard(shardInfo.id)
    }
    "be linkable and unlinkable" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      val b = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "b", "localhost")
      nameServerShard.createShard(a, repo)
      nameServerShard.createShard(b, repo)
      nameServerShard.addLink(a.id, b.id, 1)
      nameServerShard.addLink(a.id, b.id, 2)
      nameServerShard.listUpwardLinks(b.id).first.weight mustEqual 2
      nameServerShard.removeLink(a.id, b.id)
      nameServerShard.removeLink(a.id, b.id)
    }
    "be markable busy" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      nameServerShard.createShard(a, repo)
      nameServerShard.markShardBusy(a.id, shards.Busy.Busy)
      nameServerShard.markShardBusy(a.id, shards.Busy.Busy)
    }
    "sets forwarding" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      nameServerShard.createShard(a, repo)
      
      nameServerShard.setForwarding(Forwarding(0, 0, a.id))
      nameServerShard.setForwarding(Forwarding(0, 0, a.id))
    }
    
  }
}
