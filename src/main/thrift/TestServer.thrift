namespace java com.twitter.gizzard.testserver.thrift

struct TestResult {
  1: i32 key
  2: string value
  3: i32 count
}

service TestServer {
  void put(1: i32 key, 2: string value)
  list<TestResult> get(1: i32 key)
}
