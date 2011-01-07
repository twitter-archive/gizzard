package com.twitter.gizzard

import scala.collection._

trait Jsonable {
  def toMap: Map[String, Any]
}