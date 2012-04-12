package com.twitter.gizzard.nameserver

import scala.util.Random
import scala.collection.mutable
import com.twitter.gizzard.shards.RoutingNode

trait LoadBalancer {
  def balanced[T](replicas: Seq[RoutingNode[T]], readOnly: Boolean): Seq[RoutingNode[T]]
}

object LoadBalancer {
  object Fixed extends LoadBalancer {
    override def balanced[T](replicas: Seq[RoutingNode[T]], readOnly: Boolean) =
      replicas
  }

  object WeightedRandom extends LoadBalancer {
    // "threadsafe enough"
    private val random = new Random

    override def balanced[T](replicas: Seq[RoutingNode[T]], readOnly: Boolean) = {
      shuffle(if (readOnly) ReadSelector else WriteSelector, replicas)
    }

    /**
     * 1) sum all weights
     * 2) recurse on remaining portion of the array
     *   a) at each step, choose random number between 0 and the remaining weight
     *   b) select bucket/item as if weights defined a sequential range
     *   c) move selected item to head, and increment offset
     */
    private[nameserver] final def shuffle[T](
      selector: WeightSelector[T],
      input: Seq[T]
    ): Seq[T] = {
      val output = input.filter(selector(_) > 0).toBuffer
      // shuffle in place
      shuffle(selector, output, 0, output.map(selector).sum)
      output
    }

    private final def shuffle[T](
      selector: WeightSelector[T],
      buffer: mutable.Buffer[T],
      offset: Int,
      remainingWeight: Int
    ): Unit = {
      if (offset >= buffer.size - 1)
        // last item cannot move
        return
      val limit = random.nextInt(remainingWeight)
      var index = 0
      // select the first item where the sum of the weight is gt the limit
      val pop =
        buffer.indexWhere({ rn =>
          index += selector(rn)
          index > limit
        }, offset)
      assert(pop != -1, remainingWeight + ", " + limit + ", " + buffer.map(selector))
      val popped = buffer(pop)
      buffer(pop) = buffer(offset)
      buffer(offset) = popped
      // recurse
      shuffle(
        selector,
        buffer,
        offset + 1,
        remainingWeight - selector(popped)
      )
    }
  }

  type WeightSelector[T] = T => Int
  object ReadSelector extends WeightSelector[RoutingNode[_]] {
    def apply(rn: RoutingNode[_]): Int = rn.weight.read
  }
  object WriteSelector extends WeightSelector[RoutingNode[_]] {
    def apply(rn: RoutingNode[_]): Int = rn.weight.write
  }
}
