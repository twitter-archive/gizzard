package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard
import com.twitter.gizzard.shards._
import com.twitter.gizzard.ConfiguredSpecification


object NameServerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    val nameServerShard                   = mock[ShardManagerSource]
    var nameServer: NameServer            = null
    var forwarder: MultiForwarder[AnyRef] = null

    val shardInfos = (1 until 6).toList map { id =>
      new ShardInfo(ShardId("localhost", id.toString), SQL_SHARD, "a", "b", Busy.Normal)
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
    var nodes: Seq[LeafRoutingNode[AnyRef]]        = null
    var replNode: ReplicatingShard[AnyRef]         = null

    doBefore {
      expect {
        one(nameServerShard).prepareReload()
        one(nameServerShard).currentState() willReturn (Seq(nameServerState), 1L)
        2.of(shardFactory).instantiateReadOnly(shardInfos(0), 1) willReturn shard
        2.of(shardFactory).instantiate(shardInfos(0), 1) willReturn shard
        2.of(shardFactory).instantiateReadOnly(shardInfos(1), 1) willReturn shard
        2.of(shardFactory).instantiate(shardInfos(1), 1) willReturn shard
        2.of(shardFactory).instantiateReadOnly(shardInfos(2), 1) willReturn shard
        2.of(shardFactory).instantiate(shardInfos(2), 1) willReturn shard
        2.of(shardFactory).instantiateReadOnly(shardInfos(3), 1) willReturn shard
        2.of(shardFactory).instantiate(shardInfos(3), 1) willReturn shard
        one(shardFactory).instantiateReadOnly(shardInfos(4), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(4), 1) willReturn shard
      }

      nodes        = shardInfos map { new LeafRoutingNode(shardFactory, _, 1) }
      replNode     = ReplicatingShard(replicatingInfo, 1, Seq(nodes(3)))

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
        one(shardFactory).instantiateReadOnly(shardInfos(2), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(2), 1) willReturn shard
        one(shardFactory).instantiateReadOnly(shardInfos(3), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(3), 1) willReturn shard
      }

      forwarder.findShardById(shardInfos(2).id)   mustEqual Some(nodes(2))
      forwarder.findShardById(replicatingInfo.id) mustEqual Some(replNode)
    }

    "find shard by id with a shard not attached to a forwarding" in {
      val floatingShard = ShardInfo(ShardId("localhost", "floating"), SQL_SHARD, "a", "b", Busy.Normal)

      expect {
        2.of(shardFactory).instantiateReadOnly(floatingShard, 1)
        2.of(shardFactory).instantiate(floatingShard, 1)
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

    "reload nameserver configuration" in {
      // Here is a list of all the events in time order (* indicates nameserver changes):
      //  (1*) nameserver data initialized                                                (      updatedSeq = 1L)
      //  (2)  in doBefore (see above), we call reload() and load the nameserver data     (local updatedSeq = 1L)
      //  (3*) nameserver data is updated - a leaf node is added to the replicating shard (      updatedSeq = 2L)
      //  (4)  we call reloadUpdatedForwardings(1L) and update our forwarding tree        (local updatedSeq = 2L)
      //  (5)  we call reloadUpdatedForwardings(2L) but we find no change - no-op         (local updatedSeq = 2L)
      //  (6*) nameserver data is updated - it is restored back to the original state     (      updatedSeq = 3L)
      //  (7)  we call reload() and load reload the original topology                     (local updatedSeq = 3L)
      //  (8)  we call reloadUpdatedForwardings(3L) but we find no change - no-op         (local updatedSeq = 3L)

      val newLinksList = List(new LinkInfo(replicatingInfo.id, shardInfos(3).id, 1), new LinkInfo(replicatingInfo.id, shardInfos(4).id, 1))
      val newReplNode = ReplicatingShard(replicatingInfo, 1, Seq(nodes(3), nodes(4)))

      expect {
        one(nameServerShard).diffState(1L) willReturn NameServerChanges(Seq(shardForwardings(3)), Nil, 2L)
        one(nameServerShard).getShard(replicatingInfo.id)          willReturn replicatingInfo
        one(nameServerShard).listDownwardLinks(replicatingInfo.id) willReturn newLinksList
        one(nameServerShard).getShard(shardInfos(3).id)            willReturn shardInfos(3)
        one(nameServerShard).listDownwardLinks(shardInfos(3).id)   willReturn List[LinkInfo]()
        one(nameServerShard).getShard(shardInfos(4).id)            willReturn shardInfos(4)
        one(nameServerShard).listDownwardLinks(shardInfos(4).id)   willReturn List[LinkInfo]()

        one(nameServerShard).diffState(2L) willReturn NameServerChanges(Nil, Nil, 2L)

        one(nameServerShard).prepareReload()
        one(nameServerShard).currentState() willReturn (Seq(nameServerState), 3L)

        one(nameServerShard).diffState(3L) willReturn NameServerChanges(Nil, Nil, 3L)

        one(shardFactory).instantiateReadOnly(shardInfos(0), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(0), 1) willReturn shard
        one(shardFactory).instantiateReadOnly(shardInfos(1), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(1), 1) willReturn shard
        one(shardFactory).instantiateReadOnly(shardInfos(2), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(2), 1) willReturn shard
        2.of(shardFactory).instantiateReadOnly(shardInfos(3), 1) willReturn shard
        2.of(shardFactory).instantiate(shardInfos(3), 1) willReturn shard
        one(shardFactory).instantiateReadOnly(shardInfos(4), 1) willReturn shard
        one(shardFactory).instantiate(shardInfos(4), 1) willReturn shard
      }

      // This will update our nameserver state to the updated state
      nameServer.reloadUpdatedForwardings()  // (4)

      forwarder.find(1, 2) mustEqual nodes(1)
      forwarder.find(2, 1) mustEqual newReplNode

      nameServer.reloadUpdatedForwardings()  // (5)

      forwarder.find(1, 2) mustEqual nodes(1)
      forwarder.find(2, 1) mustEqual newReplNode

      // This will revert our nameserver state back to the original state
      nameServer.reload()  // (6)

      forwarder.find(1, 2) mustEqual nodes(1)
      forwarder.find(2, 1) mustEqual replNode

      nameServer.reloadUpdatedForwardings()  // (7)

      forwarder.find(1, 2) mustEqual nodes(1)
      forwarder.find(2, 1) mustEqual replNode
    }
  }
}
