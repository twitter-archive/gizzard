package com.twitter.gizzard.shards

/**
 * @param raw The base value from which write and read weights were computed.
 * @param write Write preference.
 * @param read Read preference.
 */
case class Weight(raw: Int, write: Int, read: Int)
