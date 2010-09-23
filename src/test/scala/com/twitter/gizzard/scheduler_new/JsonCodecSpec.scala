package com.twitter.gizzard.scheduler_new

import scala.collection.mutable
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.json.Json

class JsonCodecSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "JsonCodec" should {
    var unparsed = new mutable.ListBuffer[String]
    val codec = new JsonCodec({ (unparsable: Array[Byte]) => unparsed += new String(unparsable) })

    "fail gracefully" in {
      codec.inflate("gobbledygook".getBytes) must throwA[UnparsableJsonException]
    }
/* 
    "when a job fails to parse" >> {
      jobParser.parse("gobbledygook") must throwA[UnparsableJobException]
    }
    */
  }
}
