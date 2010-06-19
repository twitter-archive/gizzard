package com.twitter.gizzard.fake

import shards.ShardException
import org.specs.mock.{ClassMocker, JMocker}

class ShardInfo extends shards.ShardInfo(new ShardId(), "", "", "", shards.Busy.Normal) {
}