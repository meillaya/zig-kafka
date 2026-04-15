const std = @import("std");
const kafka = @import("zig-kafka");
const primitives = kafka.protocol.primitives;

test "primitive integer and string roundtrip" {
    var list = std.array_list.Managed(u8).init(std.testing.allocator);
    defer list.deinit();
    try primitives.appendInt(&list, i16, 7);
    try primitives.appendInt(&list, i32, 1024);
    try primitives.appendString(&list, "zig");

    var reader = primitives.Reader.init(list.items);
    try std.testing.expectEqual(@as(i16, 7), try reader.readInt(i16));
    try std.testing.expectEqual(@as(i32, 1024), try reader.readInt(i32));
    const value = try reader.readString(std.testing.allocator);
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings("zig", value);
}
