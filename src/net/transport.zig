const std = @import("std");
const primitives = @import("../protocol/primitives.zig");
const header = @import("../protocol/header.zig");
const Connection = @import("connection.zig").Connection;

pub const Transport = struct {
    allocator: std.mem.Allocator,
    client_id: []const u8,
    next_correlation_id: i32 = 1,

    pub fn init(allocator: std.mem.Allocator, client_id: []const u8) Transport {
        return .{ .allocator = allocator, .client_id = client_id };
    }

    pub fn nextCorrelationId(self: *Transport) i32 {
        const id = self.next_correlation_id;
        self.next_correlation_id += 1;
        return id;
    }

    pub fn send(self: *Transport, connection: *Connection, api_key: i16, api_version: i16, body: []const u8) ![]u8 {
        var req = std.array_list.Managed(u8).init(self.allocator);
        defer req.deinit();

        const correlation_id = self.nextCorrelationId();
        const req_header = header.RequestHeader{
            .api_key = api_key,
            .api_version = api_version,
            .correlation_id = correlation_id,
            .client_id = self.client_id,
        };
        try req_header.encode(&req);
        try req.appendSlice(body);

        var frame = std.array_list.Managed(u8).init(self.allocator);
        defer frame.deinit();
        try primitives.appendInt(&frame, i32, @intCast(req.items.len));
        try frame.appendSlice(req.items);

        try connection.stream.writeAll(frame.items);

        var size_buf: [4]u8 = undefined;
        const size_read = try connection.stream.readAtLeast(&size_buf, size_buf.len);
        if (size_read != size_buf.len) return error.UnexpectedEOF;
        const response_len = std.mem.readInt(i32, &size_buf, .big);
        if (response_len < 4) return error.InvalidData;

        const response = try self.allocator.alloc(u8, @intCast(response_len));
        errdefer self.allocator.free(response);
        const response_read = try connection.stream.readAtLeast(response, response.len);
        if (response_read != response.len) return error.UnexpectedEOF;

        var reader = primitives.Reader.init(response);
        const response_header = try header.ResponseHeader.decode(&reader);
        if (response_header.correlation_id != correlation_id) return error.CorrelationIdMismatch;

        const payload = try self.allocator.dupe(u8, response[reader.index..]);
        self.allocator.free(response);
        return payload;
    }
};
