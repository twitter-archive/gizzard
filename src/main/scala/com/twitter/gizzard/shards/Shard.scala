package com.twitter.gizzard.shards


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]

  override def toString = {
    val childrenStr = if (children.size > 0) " children=("+children.mkString(",")+")" else ""
    "<"+shardInfo.id+childrenStr+">"
  }
}
