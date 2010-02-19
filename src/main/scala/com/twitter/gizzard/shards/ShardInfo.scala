package com.twitter.gizzard.shards


case class ShardInfo(var className: String, var tablePrefix: String, var hostname: String,
                     var sourceType: String, var destinationType: String,
                     var busy: Busy.Value, var shardId: Int) {
  def this(className: String, tablePrefix: String, hostname: String) =
    this(className, tablePrefix, hostname, "", "", Busy.Normal, 0)

  def this(className: String, tablePrefix: String, hostname: String, sourceType: String,
           destinationType: String) =
    this(className, tablePrefix, hostname, sourceType, destinationType, Busy.Normal, 0)

  override def clone(): ShardInfo =
    new ShardInfo(className, tablePrefix, hostname, sourceType, destinationType, busy, shardId)
}
