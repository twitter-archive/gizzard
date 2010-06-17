package com.twitter.gizzard.shards


trait ReadWriteShard[ConcreteShard <: Shard] extends Shard {
  
  //Must override at least one read and one write

  def readOperation[A](address: Option[(Int, Long)], method: (ConcreteShard => A)): A = readOperation(method)
  def writeOperation[A](address: Option[(Int, Long)], method: (ConcreteShard => A)): A = writeOperation(method)

  def readOperation[A](address: (Int, Long), method: (ConcreteShard => A)): A = readOperation(Some(address), method)
  def writeOperation[A](address: (Int, Long), method: (ConcreteShard => A)): A = writeOperation(Some(address), method)

  def readOperation[A](method: (ConcreteShard => A)): A = readOperation(None, method)
  def writeOperation[A](method: (ConcreteShard => A)): A = writeOperation(None, method)
  
}
