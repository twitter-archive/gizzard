package com.twitter.gizzard.nameserver

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.logging.Logger
import com.twitter.util.Duration

class PollingUpdater(nameServer : NameServer, pollInterval : Duration) {
  private val log = Logger.get(getClass.getName)
  private val threadFactory = new ThreadFactoryBuilder().setDaemon(true).
      setNameFormat("gizzard-polling-updater-%d").build()
  private val scheduledExecutor = Executors.newScheduledThreadPool(1, threadFactory)
  @volatile private var lastVersion = 0L

  private[nameserver] def poll() {
    val currentVersion = try {
      nameServer.shardManager.getUpdateVersion()
    } catch {
      case e: Exception => {
        log.error(e, "[PollingUpdater] failed to read version from name server")
        lastVersion
      }
    }
    log.ifDebug("[PollingUpdater] current version %d".format(currentVersion))
    if (currentVersion > lastVersion) {
      log.info("[PollingUpdater] detected version change. Old version: %d. New version %d. Reloading config",
               lastVersion, currentVersion)
      lastVersion = currentVersion
      try {
        nameServer.reload()
      } catch {
        case e: Exception =>
          log.error(e, "[PollingUpdater] exception while reloading config")
      }
    }
  }

  def start() {
    log.info("[PollingUpdater] starting. Poll interval: %s", pollInterval)
    lastVersion = nameServer.shardManager.getUpdateVersion()
    log.info("[PollingUpdater] initial version %d", lastVersion)
    val intervalSec = pollInterval.inSeconds
    val pollTask = new Runnable() {
      override def run() {
        poll()
      }
    }
    scheduledExecutor.scheduleWithFixedDelay(pollTask, 0L, intervalSec, TimeUnit.SECONDS)
  }

  def stop() {
    scheduledExecutor.shutdown()
  }
}
