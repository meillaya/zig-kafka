const std = @import("std");
const kafka = @import("zig-kafka");

const storage_mod = kafka.server.storage;

test "mutable storage appends records and keeps existing seeded records" {
    var storage = try storage_mod.Storage.init(std.testing.allocator);
    defer storage.deinit();

    const owned = try std.testing.allocator.dupe(u8, "new-value");
    defer std.testing.allocator.free(owned);
    const append = try storage.appendRecords("broker-topic-a", 0, &.{.{ .offset = 0, .timestamp = 0, .key = null, .value = owned }});
    try std.testing.expectEqual(@as(i64, 2), append.base_offset);

    const selection = try storage.selectRecords("broker-topic-a", 0, 2);
    defer std.testing.allocator.free(selection.records);
    try std.testing.expectEqual(@as(usize, 1), selection.records.len);
    try std.testing.expectEqualStrings("new-value", selection.records[0].value.?);
}
