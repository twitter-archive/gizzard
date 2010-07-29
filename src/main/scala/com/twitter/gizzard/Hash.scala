package com.twitter.gizzard

import java.nio.{ByteBuffer, ByteOrder}


object Hash {
  object FNV1A_64 extends (Array[Byte] => Long) {
    val PRIME = 1099511628211L

    final def apply(data: Array[Byte]): Long = {
      var i = 0
      val len = data.length
      var rv = 0xcbf29ce484222325L
      while (i < len) {
        rv = (rv ^ (data(i) & 0xff)) * PRIME
        i += 1
      }
      // trim to 60 bits for gizzard.
      rv & 0x0fffffffffffffffL
    }

    final def apply(data: String): Long = apply(data.getBytes())

    final def apply(data: Long): Long = {
      val buffer = new Array[Byte](8)
      val byteBuffer = ByteBuffer.wrap(buffer)
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
      byteBuffer.putLong(data)
      apply(buffer)
    }
  }
}
