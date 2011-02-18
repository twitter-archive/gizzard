package com.twitter.gizzard
package proxy

import java.sql.SQLException
import com.twitter.gizzard.shards._
import org.specs.mock.JMocker


object SqlExceptionWrappingProxySpec extends ConfiguredSpecification with JMocker {
  "SqlExceptionWrappingProxy" should {
    val shard = mock[fake.Shard]
    val proxyFactory = new SqlExceptionWrappingProxyFactory[fake.Shard]
    val shardProxy = proxyFactory(shard)
    val shardInfo  = ShardInfo(ShardId("test", "shard"), "fake.shard", "blah", "blah", Busy.Normal)
    val sqlException = new SQLException("huh!")

    "wrap exceptions" in {
      expect {
        one(shard).get("blah") willThrow sqlException
        one(shard).shardInfo willReturn shardInfo
      }

      shardProxy.get("blah") must throwA(new ShardException(sqlException.toString, sqlException))
    }
  }
}
