import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.conversions.time._

object Priority extends Enumeration {
  val High, Medium, Low = Value
}

trait Credentials extends Connection {
  val username = "root"
  val password = ""
}

object GizzardMemoization {
  var nsQueryEvaluator: QueryEvaluatorFactory = null
}

object TestQueryEvaluator extends QueryEvaluator {
  override def apply() = {
    if (GizzardMemoization.nsQueryEvaluator == null) {
      GizzardMemoization.nsQueryEvaluator = super.apply()
    }

    GizzardMemoization.nsQueryEvaluator
  }
}

class TestScheduler(val name: String) extends Scheduler {
  val schedulerType = new KestrelScheduler {
    path = "/tmp"
    keepJournal = false
  }
  errorLimit = 25
  badJobQueue = new JsonJobLogger { name = "bad_jobs" }
}

new GizzardServer {
  val nameServer = new NameServer {
    jobRelay.priority = Priority.High.id

    val replicas = Seq(new Mysql {
      val connection = new Connection with Credentials {
        val hostnames = Seq("localhost")
        val database  = "gizzard_test"
      }
    })
  }

  val jobQueues = Map(
    Priority.High.id   -> new TestScheduler("high"),
    Priority.Medium.id -> new TestScheduler("medium"),
    Priority.Low.id    -> new TestScheduler("low")
  )


  logging = new LogConfigString("""
level = "error"
throttle_period_msec = 60000
truncate_stack_traces = 0
throttle_rate = 10
roll = "never"

exception {
  roll = "never"
  format = "exception_json"
}

w3c {
  node = "w3c"
  use_parents = false
  level = "debug"
  roll = "never"
  format = "bare"
}

bad_jobs {
  node = "bad_jobs"
  use_parents = false
  level = "info"
  roll = "never"
}
""")
}
