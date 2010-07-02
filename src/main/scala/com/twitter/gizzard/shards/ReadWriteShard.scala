package com.twitter.gizzard.shards

trait ReadWriteShard[ConcreteShard <: Shard] extends Shard {
  def readOperation[A](method: (ConcreteShard => A)): A
  def writeOperation[A](method: (ConcreteShard => A)): A
}
