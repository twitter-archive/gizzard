package com.twitter.gizzard.nameserver

import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object ShardRepositorySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "BasicShardRepository" should {
    val future = mock[Future]
    val shard = mock[shards.Shard]
    val constructor = { (shard: shards.ReadWriteShard[shards.Shard]) => shard }
    val repository = new BasicShardRepository(constructor, Some(future))

    "find a read-only shard" in {
      repository.factory("ReadOnlyShard") must haveClass[shards.ReadOnlyShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.ReadOnlyShard") must haveClass[shards.ReadOnlyShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.ReadOnlyShard") must throwA[NoSuchElementException]
    }
    "find a write-only shard" in {
      repository.factory("WriteOnlyShard") must haveClass[shards.WriteOnlyShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.WriteOnlyShard") must haveClass[shards.WriteOnlyShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.WriteOnlyShard") must throwA[NoSuchElementException]
    }
    "find a blocked shard" in {
      repository.factory("BlockedShard") must haveClass[shards.BlockedShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.BlockedShard") must haveClass[shards.BlockedShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.BlockedShard") must throwA[NoSuchElementException]
    }
    "find a replicating shard" in {
      repository.factory("ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.ReplicatingShard") must throwA[NoSuchElementException]
    }
    "find a failing over shard" in {
      repository.factory("FailingOverShard") must haveClass[shards.FailingOverShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.FailingOverShard") must haveClass[shards.FailingOverShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.FailingOverShard") must throwA[NoSuchElementException]
    }
  }
}
