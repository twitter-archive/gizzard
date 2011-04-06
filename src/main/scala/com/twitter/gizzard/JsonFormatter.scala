package com.twitter.gizzard

import com.codahale.jerkson.Json.generate
import com.twitter.logging.Formatter
import scala.collection.mutable
import java.util.logging.LogRecord

/**
 * Formatter that logs a line as structured json.
 */
object JsonFormatter extends Formatter {
  override def format(record: LogRecord) = {
    val map = mutable.Map[String, Any](
      "level" -> record.getLevel().toString(),
      "logger" -> record.getLoggerName(),
      "message" -> (record.getParameters match {
        case null => record.getMessage()
        case formatArgs => String.format(record.getMessage, formatArgs: _*)
      })
    )

    val thrown = record.getThrown()
    if (thrown != null) {
      map += ("exception" -> populateExceptionFields(thrown))
    }

    generate(map.toMap).toString + "\n"
  }

  private def populateExceptionFields(thrown: Throwable): Map[String, Any] = {
    val stacktrace = thrown.getStackTrace().map { _.toString() }
    val map = mutable.Map[String, Any](
      "message" -> thrown.getMessage(),
      "trace" -> stacktrace
    )

    val cause = thrown.getCause()
    if (cause != null) {
      map += ("cause" -> populateExceptionFields(thrown))
    }
    map.toMap
  }
}
