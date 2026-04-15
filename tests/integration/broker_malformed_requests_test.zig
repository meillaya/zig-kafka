const std = @import("std");

fn envOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}

fn integrationEnabled() !bool {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const value = try envOrDefault(allocator, "ZIG_KAFKA_RUN_INTEGRATION", "0");
    return std.mem.eql(u8, value, "1") or std.ascii.eqlIgnoreCase(value, "true");
}

fn brokerTestPort(allocator: std.mem.Allocator) !u16 {
    const value = try envOrDefault(allocator, "ZIG_KAFKA_BROKER_TEST_PORT", "19094");
    return std.fmt.parseInt(u16, value, 10);
}

fn repoRoot(allocator: std.mem.Allocator) ![]const u8 {
    return envOrDefault(allocator, "ZIG_KAFKA_REPO_ROOT", ".");
}

fn waitForBroker(host: []const u8, port: u16) !void {
    var attempts: usize = 0;
    while (attempts < 50) : (attempts += 1) {
        const stream = std.net.tcpConnectToHost(std.testing.allocator, host, port) catch {
            std.Thread.sleep(100 * std.time.ns_per_ms);
            continue;
        };
        var s = stream;
        s.close();
        return;
    }
    return error.BrokerDidNotStart;
}

fn withBrokerProcess(allocator: std.mem.Allocator, port: u16, comptime cb: anytype) !void {
    const cwd = try repoRoot(allocator);
    defer allocator.free(cwd);
    const port_text = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_text);
    const argv = [_][]const u8{ "zig", "build", "run-broker", "--", "--host", "127.0.0.1", "--port", port_text };
    var child = std.process.Child.init(&argv, allocator);
    child.cwd = cwd;
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    try child.spawn();
    defer {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
    }
    try waitForBroker("127.0.0.1", port);
    try cb(port);
}

fn expectConnectionClosedAfterWrite(port: u16, payload: []const u8) !void {
    var stream = try std.net.tcpConnectToHost(std.testing.allocator, "127.0.0.1", port);
    defer stream.close();
    try stream.writeAll(payload);
    stream.close();
}

fn expectEofOnRead(port: u16) !void {
    var stream = try std.net.tcpConnectToHost(std.testing.allocator, "127.0.0.1", port);
    defer stream.close();
    var buf: [1]u8 = undefined;
    const amt = try stream.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), amt);
}

test "broker closes cleanly on truncated frame payload" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 20;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            const payload = [_]u8{ 0, 0, 0, 10, 1, 2, 3 };
            try expectConnectionClosedAfterWrite(p, &payload);
        }
    }.run);
}

test "broker closes cleanly on incomplete request header" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 21;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            const payload = [_]u8{ 0, 0, 0, 6, 0, 18, 0, 2, 0, 0 };
            try expectConnectionClosedAfterWrite(p, &payload);
        }
    }.run);
}

test "broker closes cleanly on malformed supported-api body" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 22;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var payload = std.array_list.Managed(u8).init(std.testing.allocator);
            defer payload.deinit();
            // Frame length
            try payload.appendSlice(&[_]u8{ 0, 0, 0, 14 });
            // Request header for Produce v7 with correlation id 1 and client id len 1 + 'x'
            try payload.appendSlice(&[_]u8{ 0, 0, 0, 7, 0, 0, 0, 1, 0, 1, 'x' });
            // Malformed/incomplete body bytes
            try payload.appendSlice(&[_]u8{ 0, 0, 0 });
            try expectConnectionClosedAfterWrite(p, payload.items);
        }
    }.run);
}
