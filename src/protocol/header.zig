const std = @import("std");
const primitives = @import("primitives.zig");

pub const RequestHeader = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: []const u8,

    pub fn encode(self: RequestHeader, list: *std.array_list.Managed(u8)) !void {
        try primitives.appendInt(list, i16, self.api_key);
        try primitives.appendInt(list, i16, self.api_version);
        try primitives.appendInt(list, i32, self.correlation_id);
        try primitives.appendString(list, self.client_id);
    }
};

pub const ResponseHeader = struct {
    correlation_id: i32,

    pub fn decode(reader: *primitives.Reader) !ResponseHeader {
        return .{ .correlation_id = try reader.readInt(i32) };
    }
};

test "request header encode decode response header" {
    var list = std.array_list.Managed(u8).init(std.testing.allocator);
    defer list.deinit();

    const header = RequestHeader{ .api_key = 3, .api_version = 7, .correlation_id = 42, .client_id = "zig-kafka" };
    try header.encode(&list);

    var payload = std.array_list.Managed(u8).init(std.testing.allocator);
    defer payload.deinit();
    try primitives.appendInt(&payload, i32, 42);
    var reader = primitives.Reader.init(payload.items);
    const response = try ResponseHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 42), response.correlation_id);
}
