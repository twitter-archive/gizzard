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
    def instantiate(classname: String) =
      repository.factory(classname).instantiate(info, Weight.Default, shard)

    "find a read-only shard" in {
      instantiate("ReadOnlyShard") must haveClass[ReadOnlyShard[Any]]
      instantiate("com.twitter.gizzard.shards.ReadOnlyShard") must haveClass[ReadOnlyShard[Any]]
      repository.factory("com.example.bogis.ReadOnlyShard") must throwA[NoSuchElementException]
    }
    "find a write-only shard" in {
      instantiate("WriteOnlyShard") must haveClass[WriteOnlyShard[Any]]
      instantiate("com.twitter.gizzard.shards.WriteOnlyShard") must haveClass[WriteOnlyShard[Any]]
      repository.factory("com.example.bogis.WriteOnlyShard") must throwA[NoSuchElementException]
    }
    "find a blocked shard" in {
      instantiate("BlockedShard") must haveClass[BlockedShard[Any]]
      instantiate("com.twitter.gizzard.shards.BlockedShard") must haveClass[BlockedShard[Any]]
      repository.factory("com.example.bogis.BlockedShard") must throwA[NoSuchElementException]
    }
    "find a blackhole shard" in {
      instantiate("BlackHoleShard") must haveClass[BlackHoleShard[Any]]
      instantiate("com.twitter.gizzard.shards.BlackHoleShard") must haveClass[BlackHoleShard[Any]]
      repository.factory("com.example.bogis.BlackHoleShard") must throwA[NoSuchElementException]
    }
    "find a slave shard" in {
      instantiate("SlaveShard") must haveClass[SlaveShard[Any]]
      instantiate("com.twitter.gizzard.shards.SlaveShard") must haveClass[SlaveShard[Any]]
      repository.factory("com.example.bogis.SlaveShard") must throwA[NoSuchElementException]
    }
    "find a replicating shard" in {
      instantiate("ReplicatingShard") must haveClass[ReplicatingShard[Any]]
      instantiate("com.twitter.gizzard.shards.ReplicatingShard") must haveClass[ReplicatingShard[Any]]
      repository.factory("com.example.bogis.ReplicatingShard") must throwA[NoSuchElementException]
    }
  }
}
