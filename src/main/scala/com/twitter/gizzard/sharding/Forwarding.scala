package com.twitter.gizzard.sharding


case class Forwarding(tableId: List[Int], baseId: Long, shardId: Int)