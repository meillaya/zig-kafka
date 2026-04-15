const std = @import("std");

test {
    std.testing.refAllDecls(@import("zig-kafka"));
    _ = @import("protocol_primitives_test.zig");
    _ = @import("header_test.zig");
    _ = @import("api_versions_test.zig");
    _ = @import("metadata_codec_test.zig");
    _ = @import("record_batch_test.zig");
    _ = @import("server_api_versions_test.zig");
    _ = @import("server_request_test.zig");
    _ = @import("describe_topic_partitions_codec_test.zig");
    _ = @import("server_catalog_test.zig");
    _ = @import("produce_codec_test.zig");
    _ = @import("fetch_codec_test.zig");
}
