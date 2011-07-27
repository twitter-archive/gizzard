package com.twitter.gizzard.shards

import org.specs.Specification

object ShardIdSpec extends Specification {
  "ShardId" should {
    "validate table prefix" in {
      ShardId("hostname", "bad-table-name") must throwA[ShardException]
      ShardId("hostname", "bad.table.name") must throwA[ShardException]
      ShardId("hostname", "good_table") mustNot throwA[ShardException]
    }
  }
}
