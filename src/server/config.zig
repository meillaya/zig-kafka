const std = @import("std");

pub const Config = struct {
    allocator: std.mem.Allocator,
    host: []const u8 = "127.0.0.1",
    port: u16 = 9094,
    reuse_address: bool = true,
};
