package com.twitter.gizzard.thrift

import com.twitter.gizzard.thrift
import com.twitter.gizzard.shards
import com.twitter.gizzard.nameserver

package object conversions {
  implicit def shardIdToThrift(id: shards.ShardId): thrift.ShardId = {
    thrift.ShardId(id.hostname, id.tablePrefix)
  }

  implicit def shardIdFromThrift(id: thrift.ShardId): shards.ShardId = {
    shards.ShardId(id.hostname, id.tablePrefix)
  }

  implicit def shardIdSeqToThrift(ids: Seq[shards.ShardId]): Seq[thrift.ShardId] = {
    ids map shardIdToThrift
  }

  implicit def shardIdSeqFromThrift(ids: Seq[thrift.ShardId]): Seq[shards.ShardId] = {
    ids map shardIdFromThrift
  }


  implicit def shardInfoToThrift(info: shards.ShardInfo): thrift.ShardInfo = {
    thrift.ShardInfo(info.id, info.className, info.sourceType, info.destinationType, info.busy)
  }

  implicit def shardInfoFromThrift(info: thrift.ShardInfo): shards.ShardInfo = {
    shards.ShardInfo(info.id, info.className, info.sourceType, info.destinationType, info.busy)
  }

  implicit def shardInfoSeqToThrift(infos: Seq[shards.ShardInfo]): Seq[thrift.ShardInfo] = {
    infos map shardInfoToThrift
  }

  implicit def shardInfoSeqFromThrift(infos: Seq[thrift.ShardInfo]): Seq[shards.ShardInfo] = {
    infos map shardInfoFromThrift
  }


  implicit def intToBusy(b: Int): shards.Busy.Value = shards.Busy(b)

  implicit def busyToInt(b: shards.Busy.Value): Int = b.id


  implicit def forwardingToThrift(forwarding: nameserver.Forwarding): thrift.Forwarding = {
    thrift.Forwarding(forwarding.tableId, forwarding.baseId, forwarding.shardId)
  }

  implicit def forwardingFromThrift(forwarding: thrift.Forwarding): nameserver.Forwarding = {
    nameserver.Forwarding(forwarding.tableId, forwarding.baseId, forwarding.shardId)
  }

  implicit def forwardingSeqToThrift(forwardings: Seq[nameserver.Forwarding]): Seq[thrift.Forwarding] = {
    forwardings map forwardingToThrift
  }

  implicit def forwardingSeqFromThrift(forwardings: Seq[thrift.Forwarding]): Seq[nameserver.Forwarding] = {
    forwardings map forwardingFromThrift
  }


  implicit def linkInfoToThrift(link: shards.LinkInfo): thrift.LinkInfo = {
    thrift.LinkInfo(link.upId, link.downId, link.weight)
  }

  implicit def linkInfoFromThrift(link: thrift.LinkInfo): shards.LinkInfo = {
    shards.LinkInfo(link.upId, link.downId, link.weight)
  }

  implicit def linkInfoSeqToThrift(links: Seq[shards.LinkInfo]): Seq[thrift.LinkInfo] = {
    links map linkInfoToThrift
  }

  implicit def linkInfoSeqFromThrift(links: Seq[thrift.LinkInfo]): Seq[shards.LinkInfo] = {
    links map linkInfoFromThrift
  }


  implicit def nameServerStateToThrift(state: nameserver.NameServerState): thrift.NameServerState = {
    thrift.NameServerState(state.shards, state.links, state.forwardings, state.tableId)
  }

  implicit def nameServerStateFromThrift(state: thrift.NameServerState): nameserver.NameServerState = {
    nameserver.NameServerState(state.shards, state.links, state.forwardings, state.tableId)
  }

  implicit def nameServerStateSeqToThrift(states: Seq[nameserver.NameServerState]): Seq[thrift.NameServerState] = {
    states map nameServerStateToThrift
  }

  implicit def nameServerStateSeqFromThrift(states: Seq[thrift.NameServerState]): Seq[nameserver.NameServerState] = {
    states map nameServerStateFromThrift
  }


  implicit def hostToThrift(host: nameserver.Host): thrift.Host = {
    thrift.Host(host.hostname, host.port, host.cluster, host.status)
  }

  implicit def hostFromThrift(host: thrift.Host): nameserver.Host = {
    nameserver.Host(host.hostname, host.port, host.cluster, host.status)
  }

  implicit def hostSeqToThrift(hosts: Seq[nameserver.Host]): Seq[thrift.Host] = {
    hosts map hostToThrift
  }

  implicit def hostSeqFromThrift(hosts: Seq[thrift.Host]): Seq[nameserver.Host] = {
    hosts map hostFromThrift
  }


  implicit def hostStatusToThrift(s: nameserver.HostStatus.Value): thrift.HostStatus = thrift.HostStatus(s.id)

  implicit def hostStatusFromThrift(s: thrift.HostStatus): nameserver.HostStatus.Value = nameserver.HostStatus(s.value)
}
