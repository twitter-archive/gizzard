namespace java com.twitter.gizzard.thrift
namespace rb Gizzard

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
