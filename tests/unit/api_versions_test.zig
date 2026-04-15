const std = @import("std");
const kafka = @import("zig-kafka");
const primitives = kafka.protocol.primitives;
const api_versions = kafka.protocol.api_versions;

test "parse api versions response" {
    var bytes = std.array_list.Managed(u8).init(std.testing.allocator);
    defer bytes.deinit();
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i32, 2);
    try primitives.appendInt(&bytes, i16, 3);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i16, 7);
    try primitives.appendInt(&bytes, i16, 18);
    try primitives.appendInt(&bytes, i16, 0);
    try primitives.appendInt(&bytes, i16, 2);
    try primitives.appendInt(&bytes, i32, 0);

    var response = try api_versions.parseResponse(std.testing.allocator, bytes.items);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), response.api_versions.len);
    try std.testing.expectEqual(@as(i16, 7), try api_versions.selectVersion(response.api_versions, 3, 0, 7));
}
