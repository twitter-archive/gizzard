package com.twitter.gizzard.logging

import scala.collection.mutable
import java.util.{logging => javalog}
import com.twitter.logging.Formatter
import com.twitter.logging.config.FormatterConfig
import com.twitter.gizzard.util.Json


/**
 * Formatter that logs exceptions as json-formatted data
 */
class ExceptionJsonFormatter extends Formatter {
  private def throwableToMap(wrapped: Throwable): Map[String, Any] = {
    var pairs = List(
      ("class" -> wrapped.getClass().getName()),
      ("trace" -> wrapped.getStackTrace().map(_.toString()))
    )

    if (wrapped.getMessage() != null) {
      pairs = ("message" -> wrapped.getMessage()) :: pairs
    }


    if (wrapped.getCause() != null) {
      pairs = ("cause" -> throwableToMap(wrapped.getCause())) :: pairs
    }

    pairs.toMap
  }

  override def format(record: javalog.LogRecord) = {
    record.getThrown match {
      case null => ""
      case ex => {
        val m = throwableToMap(ex) ++ Map(
          "level" -> record.getLevel(),
          "created_at" -> (record.getMillis() / 1000)
        )

        (new String(Json.encode(m), "UTF-8")) + lineTerminator
      }
    }
  }
}

package config {
  class ExceptionJsonFormatterConfig extends FormatterConfig {
    override def apply() = new ExceptionJsonFormatter
  }
}
