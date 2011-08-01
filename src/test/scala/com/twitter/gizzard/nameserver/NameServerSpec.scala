package com.twitter.gizzard
package nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

import com.twitter.gizzard
import com.twitter.gizzard.shards._


object NameServerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    val nameServerShard                        = mock[ShardManagerSource]
    var nameServer: NameServer                 = null
    var forwarder: MultiForwarder[AnyRef] = null

    val shardInfos = (1 until 5).toList map { id =>
      new ShardInfo(ShardId("localhost", id.toString), SQL_SHARD, "a", "b", shards.Busy.Normal)
    }
    val replicatingInfo = new ShardInfo(ShardId("localhost", "replicating"), "ReplicatingShard", "", "", Busy.Normal)

    val linksList        = List(new LinkInfo(replicatingInfo.id, shardInfos(3).id, 1))
    val shardForwardings = List(
      new Forwarding(1, 1, shardInfos(0).id),
      new Forwarding(1, 2, shardInfos(1).id),
      new Forwarding(1, 3, shardInfos(2).id),
      new Forwarding(2, 1, replicatingInfo.id)
    )

    val nameServerState = NameServerState(shardInfos :+ replicatingInfo, linksList, shardForwardings, 1)

    val shard        = mock[AnyRef]
    var shardFactory = mock[ShardFactory[AnyRef]]
    val nodes        = shardInfos map { new LeafRoutingNode(shardFactory, _, 1) }
    val replNode     = ReplicatingShard(replicatingInfo, 1, Seq(nodes(3)))

    doBefore {
      expect {
        one(nameServerShard).reload()
        one(nameServerShard).currentState()    willReturn Seq(nameServerState)
      }

      nameServer = new NameServer(LeafRoutingNode(nameServerShard), identity)
      forwarder  = nameServer.configureMultiForwarder[AnyRef](
        _.shardFactories(SQL_SHARD -> shardFactory)
      )
      nameServer.reload()
    }

    // "reload and get shard info" in {
    //   nameServer.getShardInfo(shardInfos(0).id) mustEqual shardInfos(0)
    //   nameServer.getShardInfo(shardInfos(1).id) mustEqual shardInfos(1)
    //   nameServer.getShardInfo(shardInfos(2).id) mustEqual shardInfos(2)
    //   nameServer.getShardInfo(shardInfos(3).id) mustEqual shardInfos(3)
    // }

    // "get children" in {
    //   nameServer.getChildren(replicatingInfo.id).toList mustEqual linksList
    // }

    "find current forwarding" in {
      expect {
        never(shardFactory).instantiate(shardInfos(1), 1) willReturn shard
        never(shardFactory).instantiate(shardInfos(3), 1) willReturn shard
      }

      forwarder.find(1, 2) mustEqual nodes(1)
      forwarder.find(2, 1) mustEqual replNode
    }

    "find forwardings" in {
      expect {
        never(shardFactory).instantiate(shardInfos(0), 1) willReturn shard
        never(shardFactory).instantiate(shardInfos(1), 1) willReturn shard
        never(shardFactory).instantiate(shardInfos(2), 1) willReturn shard
      }

      forwarder.findAll(1) must haveTheSameElementsAs(List(nodes(0), nodes(1), nodes(2)))
    }

    "find shard by id" in {
      expect {
        one(nameServerShard).getShard(shardInfos(2).id)            willReturn shardInfos(2)
        one(nameServerShard).listDownwardLinks(shardInfos(2).id)   willReturn List[LinkInfo]()
        one(nameServerShard).getShard(replicatingInfo.id)          willReturn replicatingInfo
        one(nameServerShard).listDownwardLinks(replicatingInfo.id) willReturn linksList
        one(nameServerShard).getShard(shardInfos(3).id)            willReturn shardInfos(3)
        one(nameServerShard).listDownwardLinks(shardInfos(3).id)   willReturn List[LinkInfo]()
        never(shardFactory).instantiate(shardInfos(2), 1)          willReturn shard
        never(shardFactory).instantiate(shardInfos(3), 1)          willReturn shard
      }

      forwarder.findShardById(shardInfos(2).id)   mustEqual Some(nodes(2))
      forwarder.findShardById(replicatingInfo.id) mustEqual Some(replNode)
    }

    "find shard by id with a shard not attached to a forwarding" in {
      val floatingShard = ShardInfo(ShardId("localhost", "floating"), SQL_SHARD, "a", "b", Busy.Normal)

      expect {
        one(nameServerShard).getShard(floatingShard.id)          willReturn floatingShard
        one(nameServerShard).listDownwardLinks(floatingShard.id) willReturn List[LinkInfo]()
      }

      forwarder.findShardById(floatingShard.id) mustEqual Some(new LeafRoutingNode(shardFactory, floatingShard, 1))
    }

    "create shard" in {
      expect {
        one(nameServerShard).createShard(shardInfos(0))
        one(shardFactory).materialize(shardInfos(0))
      }
      nameServer.shardManager.createAndMaterializeShard(shardInfos(0)) mustNot throwA[InvalidShard]
    }
  }
}
