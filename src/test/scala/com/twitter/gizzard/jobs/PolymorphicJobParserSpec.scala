package com.twitter.gizzard.jobs

import net.lag.configgy.Configgy
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object PolymorphicJobParserSpec extends Specification with JMocker with ClassMocker {
  "PolymorphicJobParser" should {
    "+= & apply" in {
      val multiParser = new PolymorphicJobParser
      val a = mock[JobParser]
      val b = mock[JobParser]
      val aJson = Map("a" -> Map("b" -> 1))
      val bJson = Map("b" -> Map("b" -> 1))
      multiParser += ("a".r, a)
      multiParser += ("b".r, b)

      expect { one(a).apply(aJson) }
      multiParser(aJson)

      expect { one(b).apply(bJson) }
      multiParser(bJson)
    }
  }
}
