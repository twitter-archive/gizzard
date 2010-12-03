import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.util.TimeConversions._

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
  val debug = true
  val autoDisable = None
  val timeout = None

  val database = new Database {
    val timeout = None
    val pool = Some(new ApachePoolingDatabase {
      override val sizeMin = 1
      override val sizeMax = 1
      override val maxWait = 1.second
      override val minEvictableIdle = 60.seconds
      val testIdleMsec = 1.seconds
      override val testOnBorrow = false
    })

  }

  val query = new Query {}

  override def apply() = {
    if (GizzardMemoization.nsQueryEvaluator == null) {
      GizzardMemoization.nsQueryEvaluator = super.apply()
    }

    GizzardMemoization.nsQueryEvaluator
  }
}

class TestScheduler(val name: String) extends Scheduler {
  val schedulerType = new Kestrel {
    val queuePath = "/tmp"
    override val keepJournal = false
  }
  val threads = 1
  val errorLimit = 25
  val errorRetryDelay = 900.seconds
  val errorStrobeInterval = 30.seconds
  val perFlushItemLimit = 1000
  val jitterRate = 0.0f
  val badJobQueue = Some(new JsonJobLogger { val name = "bad_jobs" })
}

new GizzardServer {
  val nameServer = new NameServer {
    val mappingFunction = Identity
    val replicas = Seq(new Mysql {
      val connection = new Connection with Credentials {
        val hostnames = Seq("localhost")
        val database = "gizzard_test"
      }
      val queryEvaluator = TestQueryEvaluator
    })

    val jobRelay = Some(new JobRelay {
      val priority = Priority.High.id
      val framed   = true
      val timeout  = 1000.millis
    })
  }

  val jobQueues = Map(
    Priority.High.id   -> new TestScheduler("high"),
    Priority.Medium.id -> new TestScheduler("medium"),
    Priority.Low.id    -> new TestScheduler("low")
  )

  val jobInjector = new JobInjector with THsHaServer {
    val timeout = 100.milliseconds
    val idleTimeout = 60.seconds

    val threadPool = new ThreadPool {
      val name = "JobInjectorThreadPool"
      val minThreads = 1
    }
  }


  override val logging = new LogConfigString("""
level = "debug"
filename = "test.log"
throttle_period_msec = 60000
truncate_stack_traces = 0
throttle_rate = 10
roll = "never"

exception {
  filename = "exception.log"
  roll = "never"
  format = "exception_json"
}

w3c {
  node = "w3c"
  use_parents = false
  filename = "test_w3c.log"
  level = "debug"
  roll = "never"
  format = "bare"
}

bad_jobs {
  node = "bad_jobs"
  use_parents = false
  filename = "test_bad_jobs.log"
  level = "info"
  roll = "never"
}
""")
}
