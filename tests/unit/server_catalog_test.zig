const std = @import("std");
const kafka = @import("zig-kafka");

const catalog = kafka.server.catalog;

test "static catalog returns known and unknown topics deterministically" {
    const known = catalog.findTopic("broker-topic-a") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i32, 3), known.partition_count);
    try std.testing.expect(catalog.findTopic("missing-topic") == null);
}
