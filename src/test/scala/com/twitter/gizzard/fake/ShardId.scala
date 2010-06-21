package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}

class ShardId() extends shards.ShardId(String.valueOf(new java.util.Random().nextInt(100000)), "") {
  
}