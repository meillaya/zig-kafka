const std = @import("std");
const kafka = @import("zig-kafka");
const primitives = kafka.protocol.primitives;
const metadata = kafka.protocol.metadata;

test "parse metadata response with one broker topic and partition" {
    var bytes = std.array_list.Managed(u8).init(std.testing.allocator);
    defer bytes.deinit();
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendString(&bytes, "127.0.0.1");
    try primitives.appendInt(&bytes, i32, 9092);
    try primitives.appendNullableString(&bytes, null);
    try primitives.appendNullableString(&bytes, null);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendString(&bytes, "topic-a");
    try primitives.appendBool(&bytes, false);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i32, 5);
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendArrayLen(&bytes, 0);

    var cluster = try metadata.parseResponse(std.testing.allocator, bytes.items);
    defer cluster.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), cluster.brokers.len);
    try std.testing.expectEqualStrings("topic-a", cluster.topics[0].name);
    try std.testing.expectEqual(@as(i32, 1), cluster.topics[0].partitions[0].leader_id);
}

test "metadata parser fails cleanly on truncated response" {
    var bytes = std.array_list.Managed(u8).init(std.testing.allocator);
    defer bytes.deinit();
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendInt(&bytes, i32, 1);
    try primitives.appendString(&bytes, "127.0.0.1");
    // Intentionally omit the remaining broker/topic bytes.
    try std.testing.expectError(error.UnexpectedEOF, metadata.parseResponse(std.testing.allocator, bytes.items));
}
