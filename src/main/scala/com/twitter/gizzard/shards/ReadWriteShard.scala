package com.twitter.gizzard.shards


trait ReadWriteShard[ConcreteShard <: Shard] extends Shard {
  def readOperation[A](id: Long, method: (ConcreteShard => A)): A
  def writeOperation[A](id: Long, method: (ConcreteShard => A)): A
}
