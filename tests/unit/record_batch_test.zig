const std = @import("std");
const kafka = @import("zig-kafka");
const record_batch = kafka.protocol.record_batch;

test "record batch roundtrip" {
    const records = [_]record_batch.Record{
        .{ .value = "one" },
        .{ .value = "two" },
    };
    const bytes = try record_batch.encode(std.testing.allocator, &records);
    defer std.testing.allocator.free(bytes);

    const decoded = try record_batch.decode(std.testing.allocator, bytes);
    defer {
        for (decoded) |*record| record.deinit(std.testing.allocator);
        std.testing.allocator.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("one", decoded[0].value.?);
    try std.testing.expectEqualStrings("two", decoded[1].value.?);
}
