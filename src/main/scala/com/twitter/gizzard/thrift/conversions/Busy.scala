package com.twitter.gizzard
package thrift.conversions


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
