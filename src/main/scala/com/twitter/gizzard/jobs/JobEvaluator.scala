package com.twitter.gizzard.jobs


class JobEvaluator(jobs: MessageQueue) {
  override def toString() = "<JobEvaluator: %s>".format(jobs)

  def apply() {
    jobs.foreach(true, { _.apply() })
  }
}
