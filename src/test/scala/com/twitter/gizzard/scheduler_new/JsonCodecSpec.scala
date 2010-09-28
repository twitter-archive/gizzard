package com.twitter.gizzard.scheduler

import scala.collection.mutable
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json

class JsonCodecSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonCodec" should {
    var unparsed = new mutable.ListBuffer[String]
    val codec = new JsonCodec[String, JsonJob[String]]({ (unparsable: Array[Byte]) => unparsed += new String(unparsable) })

    "fail gracefully" in {
      codec.inflate("gobbledygook".getBytes) must throwA[UnparsableJsonException]
    }

    "+= & inflate" in {
      val a = mock[JsonJobParser[String, JsonJob[String]]]
      val b = mock[JsonJobParser[String, JsonJob[String]]]
      val aJson = Map("a" -> Map("b" -> 1))
      val bJson = Map("b" -> Map("b" -> 1))
      val job = mock[JsonJob[String]]
      codec += ("a".r, a)
      codec += ("b".r, b)

      expect {
        one(a).parse(codec, aJson("a")) willReturn job
      }
      codec.inflate(aJson) mustEqual job

      expect {
        one(b).parse(codec, bJson("b")) willReturn job
      }
      codec.inflate(bJson) mustEqual job
    }
  }
}
