namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

struct Job {
  1: i32 priority
  2: binary contents
  3: optional bool is_replicated
}

exception JobException {
  1: string description
}

service JobInjector {
  void inject_jobs(1: list<Job> jobs) throws(1: JobException ex)
}
