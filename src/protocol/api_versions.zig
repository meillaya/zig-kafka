const std = @import("std");
const primitives = @import("primitives.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 18;
pub const min_version: i16 = 2;
pub const max_version: i16 = 2;

pub const SupportedApiVersion = struct {
    api_key: i16,
    min_version: i16,
    max_version: i16,
};

pub const Response = struct {
    error_code: i16,
    api_versions: []SupportedApiVersion,
    throttle_time_ms: i32,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.api_versions);
        self.* = undefined;
    }
};

pub fn encodeRequest(allocator: std.mem.Allocator) ![]u8 {
    _ = allocator;
    return &.{};
}

pub fn parseResponse(allocator: std.mem.Allocator, bytes: []const u8) !Response {
    var reader = primitives.Reader.init(bytes);
    const error_code = try reader.readInt(i16);
    const count = try reader.readInt(i32);
    if (count < 0) return kafka_errors.KafkaError.NegativeLength;
    const versions = try allocator.alloc(SupportedApiVersion, @intCast(count));
    errdefer allocator.free(versions);
    for (versions) |*item| {
        item.* = .{
            .api_key = try reader.readInt(i16),
            .min_version = try reader.readInt(i16),
            .max_version = try reader.readInt(i16),
        };
    }
    const throttle = if (reader.remaining() >= 4) try reader.readInt(i32) else 0;
    return .{ .error_code = error_code, .api_versions = versions, .throttle_time_ms = throttle };
}

pub fn selectVersion(versions: []const SupportedApiVersion, target_api: i16, min_supported: i16, max_supported: i16) !i16 {
    for (versions) |entry| {
        if (entry.api_key != target_api) continue;
        const selected_min = @max(entry.min_version, min_supported);
        const selected_max = @min(entry.max_version, max_supported);
        if (selected_min > selected_max) return error.UnsupportedVersion;
        return selected_max;
    }
    return error.ApiNotSupported;
}
