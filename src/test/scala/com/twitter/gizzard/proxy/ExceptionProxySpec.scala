package com.twitter.gizzard.proxy

import java.sql.SQLException
import com.twitter.gizzard.shards._
import org.specs.mock.JMocker
import com.twitter.gizzard.ConfiguredSpecification
import com.twitter.gizzard.fake.Shard


object SqlExceptionWrappingProxySpec extends ConfiguredSpecification with JMocker {
  "SqlExceptionWrappingProxy" should {
    val shard = mock[Shard]
    val shardInfo  = ShardInfo(ShardId("test", "shard"), "fake.shard", "blah", "blah", Busy.Normal)
    val proxyFactory = new SqlExceptionWrappingProxyFactory[Shard](shardInfo.id)
    val shardProxy = proxyFactory(shard)
    val sqlException = new SQLException("huh!")

    "wrap exceptions" in {
      expect {
        one(shard).get("blah") willThrow sqlException
      }

      shardProxy.get("blah") must throwA(new ShardException(sqlException.toString, sqlException))
    }

    "wrap a readOnly exception with ShardOfflineException" in {
      expect {
        one(shard).get("blah") willThrow new SQLException("blah", "blah", 1290)
      }

      shardProxy.get("blah") must throwA(new ShardOfflineException(shardInfo.id))
    }
  }
}
