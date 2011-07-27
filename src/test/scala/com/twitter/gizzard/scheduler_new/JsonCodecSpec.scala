package com.twitter.gizzard.scheduler

import scala.collection.mutable
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json
import com.twitter.gizzard.ConfiguredSpecification


class JsonCodecSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonCodec" should {
    var unparsed = new mutable.ListBuffer[String]
    val codec = new JsonCodec({ (unparsable: Array[Byte]) => unparsed += new String(unparsable) })

    "parse some json" in {
      val jsonMap = Map("has" -> "to", "be" -> "a", "map" -> List(3, Map("like" -> "so")))

      val job = mock[JsonJob]
      val parser = mock[JsonJobParser]

      codec += ("this".r, parser)
      expect {
        one(parser).parse(jsonMap) willReturn job
      }

      val json = """{"this":{"has":"to","be":"a","map":[3, {"like":"so"}]}}"""
      codec.inflate(json.getBytes()) mustEqual job
    }

    "fail gracefully" in {
      codec.inflate("gobbledygook".getBytes) must throwA[UnparsableJsonException]
    }

    "+= & inflate" in {
      val a = mock[JsonJobParser]
      val b = mock[JsonJobParser]
      val aJson = Map("a" -> Map("b" -> 1))
      val bJson = Map("b" -> Map("b" -> 1))
      val job = mock[JsonJob]
      codec += ("a".r, a)
      codec += ("b".r, b)

      expect {
        one(a).parse(aJson("a")) willReturn job
      }
      codec.inflate(aJson) mustEqual job

      expect {
        one(b).parse(bJson("b")) willReturn job
      }
      codec.inflate(bJson) mustEqual job
    }
  }
}
