package com.twitter.gizzard.nameserver

import gizzard.shards.{ShardId, ShardInfo, LinkInfo}
import thrift.conversions.ShardInfo._
import thrift.conversions.LinkInfo._
import thrift.conversions.Forwarding._
import thrift.conversions.Sequences._
import scala.collection.mutable.ListBuffer

case class NameServerState(shards: List[ShardInfo], links: List[LinkInfo], forwardings: List[Forwarding], tableId: Int) {
  def toThrift = {
    val thriftForwardings = forwardings.map(_.toThrift).toJavaList
    val thriftLinks       = links.map(_.toThrift).toJavaList
    val thriftShards      = shards.map(_.toThrift).toJavaList
    new thrift.NameServerState(thriftShards, thriftLinks, thriftForwardings, tableId)
  }
}

object NameServerState {
  protected[nameserver] def mapOfSets[A,B](s: Iterable[A])(getKey: A => B): Map[B,Set[A]] = {
    s.foldLeft(Map[B,Set[A]]()) { (m, item) =>
      val key = getKey(item)
      m + (key -> m.get(key).map(_ + item).getOrElse(Set(item)))
    }
  }

  protected[nameserver] def descendantLinks(ids: Set[ShardId])(f: ShardId => Iterable[LinkInfo]): Set[LinkInfo] = {

    // if f is a map, just rescue and return an empty set on application in flatMap
    def getOrElse(id: ShardId) = try { f(id) } catch { case e: NoSuchElementException => Nil }

    if (ids.isEmpty) Set() else {
      val ls = ids.flatMap(getOrElse)
      ls ++ descendantLinks(ls.map(_.downId))(f)
    }
  }

  def extractTable(tableId: Int)
                  (forwardingsByTable: Int => Set[Forwarding])
                  (linksByUpId: ShardId => Set[LinkInfo])
                  (shardsById: ShardId => ShardInfo) = {

    val forwardings = forwardingsByTable(tableId)
    val links       = descendantLinks(forwardings.map(_.shardId))(linksByUpId)
    val shards      = (forwardings.map(_.shardId) ++ links.map(_.downId)).map(shardsById)

    NameServerState(shards.toList, links.toList, forwardings.toList, tableId)
  }
}
