const std = @import("std");

pub const Config = struct {
    allocator: std.mem.Allocator,
    bootstrap_brokers: []const []const u8,
    client_id: []const u8 = "zig-kafka",
    request_timeout_ms: i32 = 5_000,
    fetch_max_bytes: i32 = 1_048_576,
};
