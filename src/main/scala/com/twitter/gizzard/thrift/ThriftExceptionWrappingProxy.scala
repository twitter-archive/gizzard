package com.twitter.gizzard.thrift

import scala.reflect.Manifest
import com.twitter.gizzard.proxy.{Proxy, ExceptionHandlingProxy}
import com.twitter.gizzard.shards


object ThriftExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  e match {
    case e: shards.ShardException =>
      throw new thrift.ShardException(e.toString)
  }
})
