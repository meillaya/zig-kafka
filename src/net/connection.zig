const std = @import("std");

pub const BrokerAddress = struct {
    host: []const u8,
    port: u16,

    pub fn parse(text: []const u8) !BrokerAddress {
        const colon = std.mem.lastIndexOfScalar(u8, text, ':') orelse return error.InvalidBrokerAddress;
        const host = text[0..colon];
        if (host.len == 0) return error.InvalidBrokerAddress;
        const port = try std.fmt.parseUnsigned(u16, text[colon + 1 ..], 10);
        return .{ .host = host, .port = port };
    }
};

pub const Connection = struct {
    stream: std.net.Stream,

    pub fn connect(allocator: std.mem.Allocator, broker: BrokerAddress) !Connection {
        const stream = try std.net.tcpConnectToHost(allocator, broker.host, broker.port);
        return .{ .stream = stream };
    }

    pub fn close(self: *Connection) void {
        self.stream.close();
    }
};
