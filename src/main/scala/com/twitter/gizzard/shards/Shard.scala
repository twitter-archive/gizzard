package com.twitter.gizzard.shards


trait Shard {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[Shard]
}

trait CopyPage {
  def deflate: Map[String,Any]
}

trait CopyableShard[P <: CopyPage] extends Shard {
  type Cursor = Map[String,Any]

  def readPage(cursor: Option[Cursor], count: Int): (P, Option[Cursor])
  def readSerializedPage(cursor: Option[Cursor], count: Int): (Map[String,Any], Option[Cursor]) = {
    val (page, nextCursor) = readPage(cursor, count)
    (page.deflate, nextCursor)
  }

  def writePage(page: P): Unit
  def writeSerializedPage(page: Map[String,Any])

  def copyPage(dest: CopyableShard[P], cursor: Option[Cursor], count: Int) = {
    val (page, nextCursor) = readPage(cursor, count)
    dest.writePage(page)
    nextCursor
  }
}
