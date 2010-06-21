package com.twitter.gizzard.shards

trait ReadWriteShard[ConcreteShard <: Shard] extends Shard {
  def readOperation[A](address: Option[Address], method: (ConcreteShard => A)): A
  def writeOperation[A](address: Option[Address], method: (ConcreteShard => A)): A
}
