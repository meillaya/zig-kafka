const std = @import("std");
const kafka = @import("zig-kafka");
const primitives = kafka.protocol.primitives;
const fetch = kafka.protocol.fetch;
const record_batch = kafka.protocol.record_batch;

test "fetch request and response codec" {
    const req = try fetch.encodeRequest(std.testing.allocator, "topic-a", 0, 12, 1024 * 1024);
    defer std.testing.allocator.free(req);
    try std.testing.expect(req.len > 0);

    const records_bytes = try record_batch.encode(std.testing.allocator, &.{.{ .value = "hello" }});
    defer std.testing.allocator.free(records_bytes);

    var bytes = std.array_list.Managed(u8).init(std.testing.allocator);
    defer bytes.deinit();
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendString(&bytes, "topic-a");
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i64, 12);
    try primitives.appendInt(&bytes, i64, 12);
    try primitives.appendInt(&bytes, i32, -1);
    try primitives.appendNullableBytes(&bytes, records_bytes);

    var response = try fetch.parseResponse(std.testing.allocator, bytes.items);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), response.partitions[0].records.len);
    try std.testing.expectEqualStrings("hello", response.partitions[0].records[0].value.?);
}
