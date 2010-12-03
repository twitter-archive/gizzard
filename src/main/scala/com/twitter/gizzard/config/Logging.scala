package com.twitter.gizzard.config

import net.lag.configgy
import net.lag.configgy.Configgy
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

trait Logging {
  def apply(): Unit
}

trait ConfiggyLogging extends Logging {
  protected def configMap: configgy.ConfigMap
  def apply() { Configgy.configLogging(configMap) }
}

class LogConfigFile(path: String) extends ConfiggyLogging {
  def configMap = configgy.Config.fromFile(path)
}

class LogConfigString(s: String) extends ConfiggyLogging {
  def configMap = configgy.Config.fromString(s)
}

object NoLoggingConfig extends Logging {
  def apply() = ()
}
