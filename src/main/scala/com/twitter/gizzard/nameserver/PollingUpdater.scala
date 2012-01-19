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

  private[nameserver] def poll() {
    val (currentVersion, masterVersion) = try {
      (nameServer.shardManager.getCurrentStateVersion(),
      nameServer.shardManager.getMasterStateVersion())
    } catch {
      case e: Exception => {
        log.error(e, "[PollingUpdater] failed to read state version from name server")
        return
      }
    }
    log.ifDebug("[PollingUpdater] current version: %d, master version: %d".
        format(currentVersion, masterVersion))
    if (currentVersion < masterVersion) {
      log.info("[PollingUpdater] detected version change. Old version: %d. New version %d. Reloading config",
               currentVersion)
      try {
        nameServer.reload()
      } catch {
        case e: Exception =>
          log.error(e, "[PollingUpdater] exception while reloading config")
      }
    } else if (currentVersion > masterVersion) {
      log.critical("[PollingUpdater] current version %d is greater than master version %d".
          format(currentVersion, masterVersion))
    }
  }

  def start() {
    log.info("[PollingUpdater] starting. Poll interval: %s", pollInterval)
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
