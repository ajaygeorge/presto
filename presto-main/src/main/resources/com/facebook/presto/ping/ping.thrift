namespace java.swift com.facebook.presto.ping
struct PingRequest {
  1: string request;
}

struct PingResponse {
  1: string response;
}

service PingService {
  PingResponse ping(1: PingRequest key);
}