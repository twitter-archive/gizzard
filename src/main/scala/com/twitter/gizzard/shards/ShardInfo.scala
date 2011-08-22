package com.twitter.gizzard.shards

case class ShardInfo(
  var id: ShardId,
  var className: String,
  var sourceType: String,
  var destinationType: String,
  var busy: Busy.Value
) {

  def this(className: String, tablePrefix: String, hostname: String) = {
    this(ShardId(hostname, tablePrefix), className, "", "", Busy.Normal)
  }

  def this(
    className: String,
    tablePrefix: String,
    hostname: String,
    sourceType: String,
    destinationType: String
  ) = {
    this(ShardId(hostname, tablePrefix), className, sourceType, destinationType, Busy.Normal)
  }

  def hostname = id.hostname
  def tablePrefix = id.tablePrefix

  override def clone(): ShardInfo = {
    new ShardInfo(id, className, sourceType, destinationType, busy)
  }
}
