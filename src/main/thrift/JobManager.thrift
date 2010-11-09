namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

struct Job {
  1: i32 priority
  2: binary contents
}

exception JobException {
  1: string description
}

service JobManager {
  void retry_errors() throws(1: JobException ex)
  void stop_writes() throws(1: JobException ex)
  void resume_writes() throws(1: JobException ex)

  void retry_errors_for(1: i32 priority) throws(1: JobException ex)
  void stop_writes_for(1: i32 priority) throws(1: JobException ex)
  void resume_writes_for(1: i32 priority) throws(1: JobException ex)

  bool is_writing(1: i32 priority) throws(1: JobException ex)
}

service JobInjectionService {
  void inject_jobs(1: list<Job> jobs) throws(1: JobException ex)
}
