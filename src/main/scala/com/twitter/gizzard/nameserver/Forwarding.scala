package com.twitter.gizzard.nameserver


case class Forwarding(tableId: List[Int], baseId: Long, shardId: Int)