package com.twitter.gizzard.thrift.conversions

import com.twitter.gizzard.shards


object Busy {
  class RichBusy(busy: shards.Busy.Value) {
    def toThrift = busy.id
  }
  implicit def busyToRichBusy(busy: shards.Busy.Value) = new RichBusy(busy)

  class RichInt(busy: Int) {
    def fromThrift = shards.Busy(busy)
  }
  implicit def intToRichInt(busy: Int) = new RichInt(busy)
}
