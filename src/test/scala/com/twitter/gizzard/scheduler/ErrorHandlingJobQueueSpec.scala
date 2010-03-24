package com.twitter.gizzard.scheduler

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.xrayspecs.Eventually
import com.twitter.json.Json
import jobs.{Schedulable, Job, JobParser}
import com.twitter.ostrich.DevNullStats


object ErrorHandlingJobQueueSpec extends Specification with JMocker with ClassMocker with Eventually {
  "ErrorHandlingJobQueue" should {
    val job = mock[Job]
    val jobParser = mock[JobParser]
    val normalQueue = mock[MessageQueue[String, String]]
    val errorQueue = mock[MessageQueue[String, String]]
    val unparsableMessageQueue = mock[MessageQueue[String, String]]
    val errorHandlingConfig = ErrorHandlingConfig(1.minute, 5,
                                                  errorQueue,
                                                  mock[MessageQueue[Schedulable, Job]],
                                                  mock[MessageQueue[String, String]],
                                                  jobParser, DevNullStats)
    val errorHandlingJobQueue = new ErrorHandlingJobQueue("name", normalQueue, errorHandlingConfig)

    "retry" >> {
      expect {
        one(errorQueue).writeTo(normalQueue)
      }
      errorHandlingJobQueue.retry()
    }

    "put" >> {
      expect {
        one(job).toJson willReturn("blah")
        one(normalQueue).put("blah")
      }
      errorHandlingJobQueue.put(job)
    }

    "pause" >> {
      expect {
        one(normalQueue).pause()
        one(errorQueue).pause()
      }
      errorHandlingJobQueue.pause()
    }

    "resume" >> {
      expect {
        one(normalQueue).resume()
        one(errorQueue).resume()
      }
      errorHandlingJobQueue.resume()
    }

    "shutdown" >> {
      expect {
        one(normalQueue).shutdown()
        one(errorQueue).shutdown()
      }
      errorHandlingJobQueue.shutdown()
    }

    "isShutdown" >> {
      expect {
        allowing(normalQueue).isShutdown willReturn true
        allowing(errorQueue).isShutdown willReturn true
      }
      errorHandlingJobQueue.isShutdown mustBe true
    }

    "size" >> {
      expect {
        one(normalQueue).size willReturn 10
      }
      errorHandlingJobQueue.size mustEqual 10
    }

    "foreach" >> {
      "when there is a parsing exception" >> {
        val attributes = Map("a" -> 1)
        val job = mock[Job]
        val normalQueue = mock[MessageQueue[String, String]]
        val errorHandlingConfig = ErrorHandlingConfig(1.minute, 5,
                                                      mock[MessageQueue[String, String]],
                                                      mock[MessageQueue[Schedulable, Job]],
                                                      unparsableMessageQueue,
                                                      jobParser, DevNullStats)
        val errorHandlingJobQueue = new ErrorHandlingJobQueue("name", normalQueue, errorHandlingConfig)

        expect {
          one(normalQueue).elements willReturn(List("foo", "{\"className\":{\"a\":1}}").elements)
          one(unparsableMessageQueue).put("foo")
          one(jobParser).apply(Map("className" -> attributes)) willReturn job
          one(job).apply()
        }
        errorHandlingJobQueue.foreach(_.apply())
      }
    }
  }
}
