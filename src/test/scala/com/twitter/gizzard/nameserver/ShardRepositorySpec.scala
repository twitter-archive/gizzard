package com.twitter.gizzard.nameserver

import com.twitter.conversions.time._
import com.twitter.gizzard.shards._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.ConfiguredSpecification


object ShardRepositorySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ShardRepository" should {
    val info = new ShardInfo("", "", "")
    val shard = Seq(mock[RoutingNode[Any]])
    val repository = new ShardRepository

    "find a read-only shard" in {
      repository.factory("ReadOnlyShard").instantiate(info, 1, shard) must haveClass[ReadOnlyShard[Any]]
      repository.factory("com.twitter.gizzard.shards.ReadOnlyShard").instantiate(info, 1, shard) must haveClass[ReadOnlyShard[Any]]
      repository.factory("com.example.bogis.ReadOnlyShard") must throwA[NoSuchElementException]
    }
    "find a write-only shard" in {
      repository.factory("WriteOnlyShard").instantiate(info, 1, shard) must haveClass[WriteOnlyShard[Any]]
      repository.factory("com.twitter.gizzard.shards.WriteOnlyShard").instantiate(info, 1, shard) must haveClass[WriteOnlyShard[Any]]
      repository.factory("com.example.bogis.WriteOnlyShard") must throwA[NoSuchElementException]
    }
    "find a blocked shard" in {
      repository.factory("BlockedShard").instantiate(info, 1, shard) must haveClass[BlockedShard[Any]]
      repository.factory("com.twitter.gizzard.shards.BlockedShard").instantiate(info, 1, shard) must haveClass[BlockedShard[Any]]
      repository.factory("com.example.bogis.BlockedShard") must throwA[NoSuchElementException]
    }
    "find a blackhole shard" in {
      repository.factory("BlackHoleShard").instantiate(info, 1, shard) must haveClass[BlackHoleShard[Any]]
      repository.factory("com.twitter.gizzard.shards.BlackHoleShard").instantiate(info, 1, shard) must haveClass[BlackHoleShard[Any]]
      repository.factory("com.example.bogis.BlackHoleShard") must throwA[NoSuchElementException]
    }
    "find a slave shard" in {
      repository.factory("SlaveShard").instantiate(info, 1, shard) must haveClass[SlaveShard[Any]]
      repository.factory("com.twitter.gizzard.shards.SlaveShard").instantiate(info, 1, shard) must haveClass[SlaveShard[Any]]
      repository.factory("com.example.bogis.SlaveShard") must throwA[NoSuchElementException]
    }
    "find a replicating shard" in {
      repository.factory("ReplicatingShard").instantiate(info, 1, shard) must haveClass[ReplicatingShard[Any]]
      repository.factory("com.twitter.gizzard.shards.ReplicatingShard").instantiate(info, 1, shard) must haveClass[ReplicatingShard[Any]]
      repository.factory("com.example.bogis.ReplicatingShard") must throwA[NoSuchElementException]
    }
  }
}
