test {
    _ = @import("kafka_roundtrip_test.zig");
    _ = @import("broker_api_versions_test.zig");
    _ = @import("broker_describe_topic_partitions_test.zig");
    _ = @import("broker_fetch_static_test.zig");
    _ = @import("broker_produce_static_test.zig");
    _ = @import("broker_concurrent_clients_test.zig");
    _ = @import("broker_pipelined_requests_test.zig");
    _ = @import("broker_graceful_shutdown_test.zig");
    _ = @import("broker_error_semantics_test.zig");
    _ = @import("broker_malformed_requests_test.zig");
}
