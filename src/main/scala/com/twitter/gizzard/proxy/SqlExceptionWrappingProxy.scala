package com.twitter.gizzard.proxy

import java.sql.SQLException
import scala.reflect.Manifest
import com.twitter.querulous.query.SqlQueryTimeoutException


object SqlExceptionWrappingProxy {
  def apply[T <: AnyRef](obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      try {
        method()
      } catch {
        case e: SQLException =>
          throw new shards.ShardException(e.toString)
        case e: SqlQueryTimeoutException =>
          throw new shards.ShardTimeoutException
      }
    }
  }
}
