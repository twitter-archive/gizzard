package com.twitter.gizzard
package shards

object ShardIdSpec extends ConfiguredSpecification {
  "ShardId" should {
    "validate table prefix" in {
      ShardId("hostname", "bad-table-name") must throwA[ShardException]
      ShardId("hostname", "bad.table.name") must throwA[ShardException]
      ShardId("hostname", "good_table") mustNot throwA[ShardException]
    }
  }
}
