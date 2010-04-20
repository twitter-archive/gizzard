package com.twitter.gizzard.scheduler

import com.twitter.xrayspecs.Eventually
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{Schedulable, Job}


object JobEvaluatorThreadSpec extends ConfiguredSpecification with JMocker with Eventually {
  "JobEvaluatorThread" should {
    "pause & resume" in {
      val queue = new fake.MessageQueue
      val evaluatorThread = new JobEvaluatorThread(queue)

      queue.start()
      evaluatorThread.start()
      evaluatorThread.isShutdown must eventually(beFalse)

      queue.pause()
      evaluatorThread.pause()
      evaluatorThread.isShutdown mustBe true

      queue.start()
      evaluatorThread.resume()
      evaluatorThread.isShutdown mustBe false

      queue.pause()
      evaluatorThread.pause()
      evaluatorThread.isShutdown mustBe true
    }
  }
}
