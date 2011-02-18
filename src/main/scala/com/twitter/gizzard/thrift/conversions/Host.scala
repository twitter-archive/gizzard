package com.twitter.gizzard
package thrift.conversions



object Host {
  class RichNameServerHost(h: nameserver.Host) {
    def toThrift = new thrift.Host(h.hostname, h.port, h.cluster, h.status.toThrift)
  }
  implicit def nameServerHostToRichHost(h: nameserver.Host) = new RichNameServerHost(h)

  class RichThriftHost(h: thrift.Host) {
    def fromThrift = new nameserver.Host(h.hostname, h.port, h.cluster, h.status.fromThrift)
  }
  implicit def thriftHostToRichHost(h: thrift.Host) = new RichThriftHost(h)


  class RichNameServerHostStatus(s: nameserver.HostStatus.Value) {
    def toThrift = thrift.HostStatus.findByValue(s.id)
  }
  implicit def nameServerHostStatusToRichHostStatus(s: nameserver.HostStatus.Value) =
    new RichNameServerHostStatus(s)

  class RichThriftHostStatus(s: thrift.HostStatus) {
    def fromThrift = nameserver.HostStatus(s.getValue)
  }
  implicit def thriftHostStatusToRichHostStatus(s: thrift.HostStatus) =
    new RichThriftHostStatus(s)
}
