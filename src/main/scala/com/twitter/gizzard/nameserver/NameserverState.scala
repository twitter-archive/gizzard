package com.twitter.gizzard.nameserver

import thrift.conversions.ShardInfo._
import thrift.conversions.LinkInfo._
import thrift.conversions.Forwarding._
import thrift.conversions.Sequences._

case class NameserverState(shards: List[gizzard.shards.ShardInfo],
                           links: List[gizzard.shards.LinkInfo],
                           forwardings: List[Forwarding]) {
  def toThrift = {
    val thriftShards = shards.map(_.toThrift).toJavaList
    val thriftLinks =  links.map(_.toThrift).toJavaList
    val thriftForwardings = forwardings.map(_.toThrift).toJavaList
    new thrift.NameserverState(thriftShards, thriftLinks, thriftForwardings)
  }
}
