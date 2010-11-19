namespace java com.twitter.gizzard.testserver.thrift

service TestServer {
  void put(1: i32 key, 2: string value)
  string get(1: i32 key)
}
