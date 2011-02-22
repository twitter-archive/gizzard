package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards.ShardId


case class Forwarding(tableId: Int, baseId: Long, shardId: ShardId)
