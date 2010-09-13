package com.twitter.gizzard.nameserver

import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.configgy.Config


object ShardRepositorySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "BasicShardRepository" should {
    val future = mock[Future]
    val shard = mock[shards.Shard]
    val constructor = { (shard: shards.ReadWriteShard[shards.Shard]) => shard }
    val repository = new BasicShardRepository(constructor, future, 6.seconds)

    "find a replicating shard" in {
      repository.factory("ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[shards.Shard]]
      repository.factory("com.twitter.gizzard.shards.ReplicatingShard") must haveClass[shards.ReplicatingShardFactory[shards.Shard]]
      repository.factory("com.example.bogis.ReplicatingShard") must throwA[NoSuchElementException]
    }
  }
}
