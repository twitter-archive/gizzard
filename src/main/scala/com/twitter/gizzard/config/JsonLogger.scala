package com.twitter.gizzard
package config

import com.twitter.logging.config.FormatterConfig

object JsonFormatterConfig extends FormatterConfig {
  override def apply() = JsonFormatter
}
