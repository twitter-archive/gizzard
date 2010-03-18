package com.twitter.gizzard.nameserver


case class Forwarding(serviceId: Int, tableId: Int, baseId: Long, shardId: Int)
