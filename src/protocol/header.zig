const std = @import("std");
const primitives = @import("primitives.zig");

pub const RequestHeader = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,

    pub fn decodeRefVersion(reader: *primitives.Reader, header_version: u8) !RequestHeader {
        const api_key = try reader.readInt(i16);
        const api_version = try reader.readInt(i16);
        const correlation_id = try reader.readInt(i32);
        const client_id = switch (header_version) {
            1 => try reader.readNullableStringRef(),
            2 => blk: {
                const value = try reader.readNullableStringRef();
                try reader.skipTaggedFields();
                break :blk value;
            },
            else => return error.InvalidData,
        };
        return .{
            .api_key = api_key,
            .api_version = api_version,
            .correlation_id = correlation_id,
            .client_id = client_id,
        };
    }

    pub fn decodeRef(reader: *primitives.Reader) !RequestHeader {
        return decodeRefVersion(reader, 1);
    }

    pub fn encodeVersion(self: RequestHeader, list: *std.array_list.Managed(u8), header_version: u8) !void {
        try primitives.appendInt(list, i16, self.api_key);
        try primitives.appendInt(list, i16, self.api_version);
        try primitives.appendInt(list, i32, self.correlation_id);
        switch (header_version) {
            1 => try primitives.appendNullableString(list, self.client_id),
            2 => {
                try primitives.appendNullableString(list, self.client_id);
                try primitives.appendEmptyTaggedFields(list);
            },
            else => return error.InvalidData,
        }
    }

    pub fn encode(self: RequestHeader, list: *std.array_list.Managed(u8)) !void {
        try self.encodeVersion(list, 1);
    }
};

pub const ResponseHeader = struct {
    correlation_id: i32,

    pub fn encodeVersion(self: ResponseHeader, list: *std.array_list.Managed(u8), header_version: u8) !void {
        try primitives.appendInt(list, i32, self.correlation_id);
        if (header_version == 1) try primitives.appendEmptyTaggedFields(list);
    }

    pub fn encode(self: ResponseHeader, list: *std.array_list.Managed(u8)) !void {
        try self.encodeVersion(list, 0);
    }

    pub fn decodeVersion(reader: *primitives.Reader, header_version: u8) !ResponseHeader {
        const correlation_id = try reader.readInt(i32);
        if (header_version == 1) try reader.skipTaggedFields();
        return .{ .correlation_id = correlation_id };
    }

    pub fn decode(reader: *primitives.Reader) !ResponseHeader {
        return decodeVersion(reader, 0);
    }
};

test "request header encode decode response header" {
    var list = std.array_list.Managed(u8).init(std.testing.allocator);
    defer list.deinit();

    const req_header = RequestHeader{ .api_key = 3, .api_version = 7, .correlation_id = 42, .client_id = "zig-kafka" };
    try req_header.encode(&list);
    var req_reader = primitives.Reader.init(list.items);
    const parsed = try RequestHeader.decodeRef(&req_reader);
    try std.testing.expectEqual(@as(i16, 3), parsed.api_key);
    try std.testing.expectEqual(@as(i32, 42), parsed.correlation_id);
    try std.testing.expectEqualStrings("zig-kafka", parsed.client_id.?);

    var payload = std.array_list.Managed(u8).init(std.testing.allocator);
    defer payload.deinit();
    const resp_header = ResponseHeader{ .correlation_id = 42 };
    try resp_header.encode(&payload);
    var reader = primitives.Reader.init(payload.items);
    const response = try ResponseHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 42), response.correlation_id);
}
