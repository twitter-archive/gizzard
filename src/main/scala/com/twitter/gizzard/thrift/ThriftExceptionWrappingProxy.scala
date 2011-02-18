package com.twitter.gizzard
package thrift

import scala.reflect.Manifest
import proxy.{Proxy, ExceptionHandlingProxy}


object ThriftExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  e match {
    case e: shards.ShardException =>
      throw new thrift.GizzardException(e.toString)
  }
})
