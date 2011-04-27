package com.twitter.gizzard
package nameserver

import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

import com.twitter.gizzard.shards.RoutingNode


object ShardRepositorySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "BasicShardRepository" should {
    val future = mock[Future]
    val shard = mock[RoutingNode[AnyRef]]
    val repository = new BasicShardRepository(Some(future))

    "find a read-only shard" in {
      repository.factory("ReadOnlyShard") must haveClass[shards.ReadOnlyShardFactory[AnyRef]]
      repository.factory("com.twitter.gizzard.shards.ReadOnlyShard") must haveClass[shards.ReadOnlyShardFactory[AnyRef]]
      repository.factory("com.example.bogis.ReadOnlyShard") must throwA[NoSuchElementException]
    }
    "find a write-only shard" in {
      repository.factory("WriteOnlyShard") must haveClass[shards.WriteOnlyShardFactory[AnyRef]]
      repository.factory("com.twitter.gizzard.shards.WriteOnlyShard") must haveClass[shards.WriteOnlyShardFactory[AnyRef]]
      repository.factory("com.example.bogis.WriteOnlyShard") must throwA[NoSuchElementException]
    }
    "find a blocked shard" in {
      repository.factory("BlockedShard") must haveClass[shards.BlockedShardFactory[AnyRef]]
      repository.factory("com.twitter.gizzard.shards.BlockedShard") must haveClass[shards.BlockedShardFactory[AnyRef]]
      repository.factory("com.example.bogis.BlockedShard") must throwA[NoSuchElementException]
    }
    "find a replicating shard" in {
      repository.factory("ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[AnyRef]]
      repository.factory("com.twitter.gizzard.shards.ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[AnyRef]]
      repository.factory("com.example.bogis.ReplicatingShard") must throwA[NoSuchElementException]
    }
    "find a failing over shard" in {
      repository.factory("FailingOverShard") must haveClass[shards.FailingOverShardFactory[AnyRef]]
      repository.factory("com.twitter.gizzard.shards.FailingOverShard") must haveClass[shards.FailingOverShardFactory[AnyRef]]
      repository.factory("com.example.bogis.FailingOverShard") must throwA[NoSuchElementException]
    }
  }
}
