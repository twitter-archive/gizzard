package com.twitter.gizzard.nameserver

import shards.ShardId


case class Forwarding(tableId: Int, baseId: Long, shardId: ShardId)
