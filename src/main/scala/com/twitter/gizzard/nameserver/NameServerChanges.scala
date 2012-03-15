package com.twitter.gizzard.nameserver

/**
 * Changes to NameServer State
 *
 * @param updatedForwardings Forwarding entries that have been added or updated
 * @param deletedForwardings Forwarding entries that have been marked "deleted"
 * @param updatedSeq         This class contains all the changes up to this updatedSeq. Since this
 *                           class is not guaranteed to be built atomically, it may contain some
 *                           extra newer changes.
 */
case class NameServerChanges(updatedForwardings: Seq[Forwarding],
                             deletedForwardings: Seq[Forwarding],
                             updatedSeq: Long)
