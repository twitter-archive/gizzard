package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.thrift.conversions.ShardId._

object CopyDestination {
  class RichCopyDestination(dest: scheduler.CopyDestination) {
    def toThrift = {
      val out = new thrift.CopyDestination(dest.shardId.toThrift)
      dest.baseId.foreach { out.setBase_id(_) }
      out
    }
  }
  implicit def destToRichCopyDestination(dest: scheduler.CopyDestination) = new RichCopyDestination(dest)

  class RichThriftCopyDestination(dest: thrift.CopyDestination) {
    def fromThrift = {
      val baseId = if(dest.isSetBase_id) Some(dest.base_id) else None
      new scheduler.CopyDestination(dest.shard_id.fromThrift, baseId)
    }
  }
  implicit def thriftCopyDestinationToRichCopyDestination(dest: thrift.CopyDestination) = new RichThriftCopyDestination(dest)
}
