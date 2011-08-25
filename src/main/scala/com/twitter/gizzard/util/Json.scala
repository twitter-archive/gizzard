package com.twitter.gizzard.util

import scala.collection.mutable
import scala.collection.JavaConversions._
import org.codehaus.jackson.map.ObjectMapper
import java.util.{Map => JMap, List => JList}


object Json {
  private val mapper = new ObjectMapper

  def encode(m: Map[String, Any]) = {
    val jmap = asJMap(m)
    mapper.writeValueAsBytes(jmap)
  }

  def decode(arr: Array[Byte]) = {
    val str = new String(arr, "UTF-8")
    val jmap: JMap[String, Any] = mapper.readValue(str, classOf[JMap[String, Any]])
    asScalaMap(jmap)
  }

  // helpers

  private def asJMap(sm: Map[String, Any]): JMap[String, Any] = {
    val jmap = new java.util.LinkedHashMap[String, Any]

    sm map { case (key, v) =>
      v match {
        case m: Map[String, Any] => jmap.put(key, asJMap(m))
        case i: Iterable[Any]    => jmap.put(key, asJList(i))
        case o                   => jmap.put(key, o)
      }
    }

    jmap
  }

  private def asJList(si: Iterable[Any]): JList[Any] = {
    val jlist = new java.util.LinkedList[Any]

    si map {
      case m: Map[String, Any] => jlist.add(asJMap(m))
      case i: Iterable[Any]    => jlist.add(asJList(i))
      case o                   => jlist.add(o)
    }

    jlist
  }

  private def asScalaMap(jmap: JMap[String, Any]): Map[String, Any] = {
    jmap map { case (key, v) =>
      v match {
        case m: JMap[String, Any] => (key -> asScalaMap(m))
        case l: JList[Any]        => (key -> asScalaList(l))
        case o                    => (key -> o)
      }
    } toMap
  }

  private def asScalaList(jlist: JList[Any]): List[Any] = {
    jlist map {
      case m: JMap[String, Any] => asScalaMap(m)
      case l: JList[Any]        => asScalaList(l)
      case o                    => o
    } toList
  }
}
