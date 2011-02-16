package com.twitter.gizzard
package nameserver

object FnvHasher extends (Long => Long) {
  def apply(number: Long) = Hash.FNV1A_64(number)
}
