package com.twitter.gizzard.thrift

import scala.reflect.Manifest
import com.twitter.gizzard.shards.ShardException
import com.twitter.gizzard.proxy.{Proxy, ExceptionHandlingProxy}


object ThriftExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  e match {
    case e: ShardException =>
      throw new GizzardException(e.toString)
  }
})
