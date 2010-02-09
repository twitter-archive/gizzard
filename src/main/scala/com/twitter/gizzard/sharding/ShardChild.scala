package com.twitter.gizzard.sharding


case class ChildInfo(shardId: Int, position: Int, weight: Int)

object ChildInfo {
  def fromThrift(info: thrift.ChildInfo) = new ChildInfo(info.shard_id, info.position, info.weight)
  def toThrift(info: ChildInfo) = new thrift.ChildInfo(info.shardId, info.position, info.weight)
}
