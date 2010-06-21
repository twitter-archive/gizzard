package com.twitter.gizzard.shards


case class LinkInfo(upId: ShardId, downId: ShardId, weight: Int)
