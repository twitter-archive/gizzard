package com.twitter.gizzard.shards
import org.specs.mock.{ClassMocker, JMocker}

object ShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  
  val s1 = mock[Shard]
  val s2 = mock[Shard]
  val s3 = mock[Shard]
  val s4 = mock[Shard]
  val s5 = mock[Shard]
  
  expect {
    s1.children willReturn Seq(s2, s3)
  }

  "Shard" should {
    "#isSelfOrDecendant" in {
      "returns true for self" in {
        s1.equalsOrContains(s1) mustEqual true
      }
      
      "returns true for descendant" in {
        s1.equalsOrContains(s3) mustEqual true
      }

      "returns false for ancestor" in {
        s3.equalsOrContains(s1) mustEqual false
      }
      
      "returns false for unrelated" in {
        s1.equalsOrContains(s5) mustEqual false
      }
    }
  }
}
