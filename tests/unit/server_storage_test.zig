const std = @import("std");
const kafka = @import("zig-kafka");

const storage_mod = kafka.server.storage;

test "static storage selects records by offset and reports beyond-end" {
    var storage = try storage_mod.Storage.init(std.testing.allocator);
    defer storage.deinit();
    const sel = try storage.selectRecords("broker-topic-a", 0, 0);
    defer std.testing.allocator.free(sel.records);
    try std.testing.expectEqual(@as(i64, 2), sel.high_watermark);
    try std.testing.expectEqual(@as(usize, 2), sel.records.len);

    const empty = try storage.selectRecords("broker-topic-a", 0, 99);
    defer std.testing.allocator.free(empty.records);
    try std.testing.expectEqual(@as(usize, 0), empty.records.len);
}
