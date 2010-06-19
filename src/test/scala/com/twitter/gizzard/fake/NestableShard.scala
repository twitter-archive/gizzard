package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}

class NestableShard(val children: Seq[Shard]) extends Shard {
  val weight = 1
  val shardInfo = new ShardInfo
  var name = ""
  
  def getName(): String = {
    name
  }
  
  def setName(tmp: String) = {
    name = tmp
  }
  
}