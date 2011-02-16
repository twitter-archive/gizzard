package com.twitter.gizzard
package nameserver

import org.specs.Specification

object FnvHasherSpec extends ConfiguredSpecification {
  "FnvHasher" should {
    "hash" in {
      FnvHasher(0L) mustEqual 632747166973704645L
      FnvHasher(1L) mustEqual 706274769219809188L
      FnvHasher(23L) mustEqual 829809842381192562L
      FnvHasher(1000L) mustEqual 964653788044945116L
      FnvHasher(0xabcdL) mustEqual 441210104309327041L
      FnvHasher(0x01234567fedcba98L) mustEqual 24648690491651621L
      FnvHasher(0x41234567fedcba98L) mustEqual 24578321747446117L
    }
  }
}
