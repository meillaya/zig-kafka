const std = @import("std");
const kafka = @import("zig-kafka");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var host: []const u8 = "127.0.0.1";
    var port: u16 = 9094;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--host")) {
            host = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--port")) {
            const port_text = args.next() orelse return error.InvalidArgument;
            port = try std.fmt.parseInt(u16, port_text, 10);
        } else {
            return error.InvalidArgument;
        }
    }

    var broker = try kafka.server.Broker.init(.{
        .allocator = allocator,
        .host = host,
        .port = port,
    });
    defer broker.deinit();

    std.debug.print("broker listening on {s}:{d}\n", .{ host, broker.localPort() });
    try broker.run();
}
