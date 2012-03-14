package com.twitter.gizzard.nameserver

/**
 * Changes to NameServer State
 *
 * @param updatedForwardings Forwarding entries that has been added or updated
 * @param deletedForwardings Forwarding entries that has been marked "deleted"
 * @param updatedSeq         This class contains all the changes up to this updatedSeq and possibly more
 */
case class NameServerChanges(updatedForwardings: Seq[Forwarding],
                             deletedForwardings: Seq[Forwarding],
                             updatedSeq: Long)
