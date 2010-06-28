package com.twitter.gizzard.shards
import org.specs.mock.{ClassMocker, JMocker}

object ShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  
  val s2 = new fake.NestableShard(Seq())
  val s3 = new fake.NestableShard(Seq())
  val s4 = new fake.NestableShard(Seq())
  val s5 = new fake.NestableShard(Seq())
  val s1 = new fake.NestableShard(Seq(s2, s3))
  

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
