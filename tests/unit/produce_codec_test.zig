const std = @import("std");
const kafka = @import("zig-kafka");
const primitives = kafka.protocol.primitives;
const produce = kafka.protocol.produce;
const record_batch = kafka.protocol.record_batch;

test "produce request and response codec" {
    const req = try produce.encodeRequest(std.testing.allocator, "topic-a", 0, &.{.{ .value = "hello" }}, 1000);
    defer std.testing.allocator.free(req);
    try std.testing.expect(req.len > 0);

    var bytes = std.array_list.Managed(u8).init(std.testing.allocator);
    defer bytes.deinit();
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendString(&bytes, "topic-a");
    try primitives.appendArrayLen(&bytes, 1);
    try primitives.appendInt(&bytes, i32, 0);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i64, 12);
    try primitives.appendInt(&bytes, i64, -1);
    try primitives.appendInt(&bytes, i64, 0);
    try primitives.appendInt(&bytes, i32, 0);

    var response = try produce.parseResponse(std.testing.allocator, bytes.items);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(i64, 12), response.partitions[0].base_offset);
}
