package com.twitter.gizzard.thrift.conversions


object Busy {
  class RichBusy(busy: sharding.Busy.Value) {
    def toThrift = busy.id
  }
  implicit def busyToRichBusy(busy: sharding.Busy.Value) = new RichBusy(busy)

  class RichInt(busy: Int) {
    def fromThrift = sharding.Busy(busy)
  }
  implicit def intToRichInt(busy: Int) = new RichInt(busy)
}
