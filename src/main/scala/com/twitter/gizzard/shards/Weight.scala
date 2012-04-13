package com.twitter.gizzard.shards

import com.twitter.gizzard.thrift.HostWeightInfo

/**
 * @param raw The base value from which write and read weights were computed.
 * @param write Write preference.
 * @param read Read preference.
 */
case class Weight(raw: Int, write: Double, read: Double)

object Weight {
  val Default = Weight(1, 1.0, 1.0)

  /** Materialize a Weight from a HostWeight. */
  def apply(raw: Int, hostWeight: Option[HostWeightInfo]): Weight = {
    val weightRead = raw * hostWeight.map(_.weight_read).getOrElse(1.0)
    val weightWrite = raw * hostWeight.map(_.weight_write).getOrElse(1.0)
    Weight(raw, weightWrite, weightRead)
  }
}
