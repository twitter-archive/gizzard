package com.twitter.gizzard.shards

case class RemoteShardId(id: ShardId, cluster: String) {
  override def toString = cluster + "/" + id.toString
}

case class ShardId(val hostname: String, val tablePrefix: String) {
  validateTablePrefix()

  private def validateTablePrefix() {
    val pattern = """([.-])""".r
    pattern.findFirstMatchIn(tablePrefix).foreach { badChar =>
      throw new ShardException("Invalid character in table prefix: " + badChar)
    }
  }

  override def toString = hostname + "/" + tablePrefix
}
