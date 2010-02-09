package com.twitter.gizzard.sharding


case class ShardInfo(className: String, tablePrefix: String, hostname: String, sourceType: String,
                     destinationType: String, busy: Busy.Value, var shardId: Int) {
  def this(className: String, tablePrefix: String, hostname: String) =
    this(className, tablePrefix, hostname, "", "", Busy.Normal, 0)
}

object ShardInfo {
  def fromThrift(shardInfo: thrift.ShardInfo) = {
    new ShardInfo(shardInfo.class_name, shardInfo.table_prefix, shardInfo.hostname,
                  shardInfo.source_type, shardInfo.destination_type,
                  Busy.fromThrift(shardInfo.busy), shardInfo.shard_id)
  }

  def toThrift(shardInfo: ShardInfo) = {
    new thrift.ShardInfo(shardInfo.className, shardInfo.tablePrefix, shardInfo.hostname,
                         shardInfo.sourceType, shardInfo.destinationType,
                         Busy.toThrift(shardInfo.busy), shardInfo.shardId)
  }
}
