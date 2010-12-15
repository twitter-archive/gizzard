package com.twitter.gizzard.shards


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
}

trait CopyPage {
  def deflate: Map[String,Any]
}

trait CopyableShard[P <: CopyPage, S <: CopyableShard[P,S]] extends Shard {
  def readPage(cursor: Option[Map[String,Any]], count: Int): (P, Option[Map[String,Any]])
  def readSerializedPage(cursor: Option[Map[String,Any]], count: Int): (Map[String,Any], Option[Map[String,Any]]) = {
    val (page, nextCursor) = readPage(cursor, count)
    (page.deflate, nextCursor)
  }

  def writePage(page: P): Unit
  def writeSerializedPage(page: Map[String,Any])

  def copyPage(dest: S, cursor: Option[Map[String,Any]], count: Int) = {
    val (page, nextCursor) = readPage(cursor, count)
    dest.writePage(page)
    nextCursor
  }
}
