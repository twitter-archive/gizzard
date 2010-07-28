package com.twitter.gizzard.nameserver

import java.util.Random

trait IdGenerator {
  def apply(): Int
}

object RandomIdGenerator extends IdGenerator {
  private val random = new Random()
  def apply() = (random.nextInt() & ((1 << 30) - 1)).toInt
}
