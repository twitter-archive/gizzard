package com.twitter.gizzard.jobs

import scala.collection.mutable
import com.twitter.json.Json
import net.lag.configgy.Configgy
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import shards.ShardRejectedOperationException


object SchedulableWithTasksSpec extends Specification with JMocker with ClassMocker {
  "SchedulableWithTasks" should {
    "to and from json" >> {
      val schedulables = List(mock[Schedulable], mock[Schedulable])
      for (schedulable <- schedulables) {
        expect {
          allowing(schedulable).className willReturn "Task"
          allowing(schedulable).toMap willReturn Map("a" -> 1)
        }
      }
      val schedulableWithTasks = new SchedulableWithTasks(schedulables)
      val json = schedulableWithTasks.toJson
      json mustMatch "SchedulableWithTasks"
      json mustMatch "\"tasks\":"
      json mustMatch "\\{\"Task\":\\{\"a\":1\\}\\}"
    }

    "loggingName" >> {
      val schedulable1 = mock[Schedulable]
      val schedulable2 = mock[Schedulable]
      val schedulableWithTasks = new SchedulableWithTasks(List(schedulable1, schedulable2))
      expect {
        allowing(schedulable1).loggingName willReturn "Schedulable1"
        allowing(schedulable2).className willReturn "Schedulable2"
        allowing(schedulable2).loggingName willReturn "Schedulable2"
      }
      schedulableWithTasks.loggingName mustEqual "Schedulable1,Schedulable2"
    }

    "equals" >> {}
  }
}
