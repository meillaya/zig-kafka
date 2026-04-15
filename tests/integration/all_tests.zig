test {
    _ = @import("kafka_roundtrip_test.zig");
    _ = @import("broker_api_versions_test.zig");
    _ = @import("broker_describe_topic_partitions_test.zig");
    _ = @import("broker_fetch_static_test.zig");
    _ = @import("broker_produce_static_test.zig");
    _ = @import("broker_concurrent_clients_test.zig");
    _ = @import("broker_pipelined_requests_test.zig");
}
