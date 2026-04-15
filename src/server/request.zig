const std = @import("std");
const header = @import("../protocol/header.zig");

pub const ParsedRequest = struct {
    frame: []u8,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: []const u8,
    body: []const u8,

    pub fn deinit(self: *ParsedRequest, allocator: std.mem.Allocator) void {
        allocator.free(self.frame);
        self.* = undefined;
    }
};

pub fn readFrame(allocator: std.mem.Allocator, stream: *std.net.Stream) ![]u8 {
    var size_buf: [4]u8 = undefined;
    const size_read = try stream.readAtLeast(&size_buf, size_buf.len);
    if (size_read != size_buf.len) return error.UnexpectedEOF;
    const frame_len = std.mem.readInt(i32, &size_buf, .big);
    if (frame_len < 0) return error.NegativeLength;
    const frame = try allocator.alloc(u8, @intCast(frame_len));
    errdefer allocator.free(frame);
    const body_read = try stream.readAtLeast(frame, frame.len);
    if (body_read != frame.len) return error.UnexpectedEOF;
    return frame;
}

pub fn parseOwnedFrame(frame: []u8) !ParsedRequest {
    var reader = kafka.protocol.primitives.Reader.init(frame);
    const req_header = try header.RequestHeader.decodeRef(&reader);
    const body = frame[reader.index..];
    return .{
        .frame = frame,
        .api_key = req_header.api_key,
        .api_version = req_header.api_version,
        .correlation_id = req_header.correlation_id,
        .client_id = req_header.client_id,
        .body = body,
    };
}

pub fn writeFrame(stream: *std.net.Stream, payload: []const u8) !void {
    var size_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &size_buf, @intCast(payload.len), .big);
    try stream.writeAll(&size_buf);
    try stream.writeAll(payload);
}

const kafka = @import("../lib.zig");
