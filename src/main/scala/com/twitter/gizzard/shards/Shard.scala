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

  def readPage(startCursor: Option[Cursor], count: Option[Int]): (P, Option[Cursor])
  def writePage(page: P)
  def writePage(page: Map[String,Any])
}

