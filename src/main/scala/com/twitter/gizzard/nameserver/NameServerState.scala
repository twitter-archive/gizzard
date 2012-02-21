package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards.{ShardId, ShardInfo, LinkInfo}
import com.twitter.gizzard.util.TreeUtils

case class NameServerState(shards: Seq[ShardInfo], links: Seq[LinkInfo], forwardings: Seq[Forwarding], tableId: Int)

object NameServerState {
  import TreeUtils._

  def extractTable(tableId: Int)
                  (forwardingsByTable: Int => Set[Forwarding])
                  (linksByUpId: ShardId => Set[LinkInfo])
                  (shardsById: ShardId => ShardInfo) = {

    val forwardings = forwardingsByTable(tableId)
    val links       = descendantLinks(forwardings.map(_.shardId))(linksByUpId)
    val shards      = (forwardings.map(_.shardId) ++ links.map(_.downId)).map(shardsById)

    NameServerState(shards.toSeq, links.toSeq, forwardings.toSeq, tableId)
  }
}
