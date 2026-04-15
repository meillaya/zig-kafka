const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
const fetch = kafka.protocol.fetch;
const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;
const produce = kafka.protocol.produce;

fn envOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}
fn boolEnvOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: bool) !bool {
    const value = try envOrDefault(allocator, name, if (default_value) "1" else "0");
    defer allocator.free(value);
    if (std.ascii.eqlIgnoreCase(value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on")) return true;
    if (std.ascii.eqlIgnoreCase(value, "0") or std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "no") or std.ascii.eqlIgnoreCase(value, "off")) return false;
    return error.InvalidBooleanEnvironmentValue;
}
fn integrationEnabled() !bool {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    return boolEnvOrDefault(arena.allocator(), "ZIG_KAFKA_RUN_INTEGRATION", false);
}
fn brokerTestPort(allocator: std.mem.Allocator) !u16 {
    const value = try envOrDefault(allocator, "ZIG_KAFKA_BROKER_TEST_PORT", "19094");
    defer allocator.free(value);
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
fn withBrokerProcess(allocator: std.mem.Allocator, port: u16, delay_ms: u32, comptime cb: anytype) !void {
    const cwd = try repoRoot(allocator);
    defer allocator.free(cwd);
    const port_text = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_text);
    const delay_text = try std.fmt.allocPrint(allocator, "{d}", .{delay_ms});
    defer allocator.free(delay_text);
    const argv = [_][]const u8{ "zig", "build", "run-broker", "--", "--host", "127.0.0.1", "--port", port_text, "--request-delay-ms", delay_text };
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
fn sendApiVersions(allocator: std.mem.Allocator, port: u16, correlation_id: i32) !void {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();
    const body = try api_versions.encodeRequest(allocator);
    defer if (body.len != 0) allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{ .api_key = api_versions.api_key, .api_version = api_versions.max_version, .correlation_id = correlation_id, .client_id = "api-it" };
    try req_header.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);
    var size_buf: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size_buf, size_buf.len);
}
fn sendProduce(allocator: std.mem.Allocator, port: u16, correlation_id: i32, value: []const u8) !i64 {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();
    const recs = [_]kafka.protocol.record_batch.Record{.{ .value = value }};
    const body = try produce.encodeRequest(allocator, "broker-topic-a", 0, &recs, 1000);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{ .api_key = produce.api_key, .api_version = produce.max_version, .correlation_id = correlation_id, .client_id = "prod-it" };
    try req_header.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);
    var size_buf: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size_buf, size_buf.len);
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    const response_payload = try allocator.alloc(u8, @intCast(payload_len));
    defer allocator.free(response_payload);
    _ = try stream.readAtLeast(response_payload, response_payload.len);
    var reader = primitives.Reader.init(response_payload);
    _ = try header.ResponseHeader.decode(&reader);
    var response = try produce.parseResponse(allocator, response_payload[reader.index..]);
    defer response.deinit(allocator);
    return response.partitions[0].base_offset;
}
fn sendFetch(allocator: std.mem.Allocator, port: u16, correlation_id: i32, offset: i64) !fetch.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();
    const body = try fetch.encodeRequest(allocator, "broker-topic-a", 0, offset, 1024 * 1024);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{ .api_key = fetch.api_key, .api_version = fetch.max_version, .correlation_id = correlation_id, .client_id = "fetch-it" };
    try req_header.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);
    var size_buf: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size_buf, size_buf.len);
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    const response_payload = try allocator.alloc(u8, @intCast(payload_len));
    defer allocator.free(response_payload);
    _ = try stream.readAtLeast(response_payload, response_payload.len);
    var reader = primitives.Reader.init(response_payload);
    _ = try header.ResponseHeader.decode(&reader);
    return try fetch.parseResponse(allocator, response_payload[reader.index..]);
}

const StartGate = struct { flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false) };
const ParallelApiCtx = struct { port: u16, gate: *StartGate, ok: bool = false };
fn parallelApi(ctx: *ParallelApiCtx) void {
    while (!ctx.gate.flag.load(.acquire)) std.Thread.yield() catch {};
    sendApiVersions(std.heap.smp_allocator, ctx.port, @intCast(std.time.nanoTimestamp() & 0x7fffffff)) catch return;
    ctx.ok = true;
}
const ParallelProduceCtx = struct { port: u16, gate: *StartGate, value: []const u8, offset: i64 = -1 };
fn parallelProduce(ctx: *ParallelProduceCtx) void {
    while (!ctx.gate.flag.load(.acquire)) std.Thread.yield() catch {};
    ctx.offset = sendProduce(std.heap.smp_allocator, ctx.port, @intCast(std.time.nanoTimestamp() & 0x7fffffff), ctx.value) catch -1;
}

test "broker serves truly parallel ApiVersions clients" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 12;
    try withBrokerProcess(allocator, port, 300, struct {
        fn run(p: u16) !void {
            var gate = StartGate{};
            var a = ParallelApiCtx{ .port = p, .gate = &gate };
            var b = ParallelApiCtx{ .port = p, .gate = &gate };
            const t1 = try std.Thread.spawn(.{}, parallelApi, .{&a});
            const t2 = try std.Thread.spawn(.{}, parallelApi, .{&b});
            const start = std.time.milliTimestamp();
            gate.flag.store(true, .release);
            t1.join();
            t2.join();
            const elapsed = std.time.milliTimestamp() - start;
            try std.testing.expect(a.ok and b.ok);
            try std.testing.expect(elapsed < 550);
        }
    }.run);
}

test "broker handles parallel produce clients and preserves combined state" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 13;
    try withBrokerProcess(allocator, port, 150, struct {
        fn run(p: u16) !void {
            var gate = StartGate{};
            var a = ParallelProduceCtx{ .port = p, .gate = &gate, .value = "concurrent-a" };
            var b = ParallelProduceCtx{ .port = p, .gate = &gate, .value = "concurrent-b" };
            const t1 = try std.Thread.spawn(.{}, parallelProduce, .{&a});
            const t2 = try std.Thread.spawn(.{}, parallelProduce, .{&b});
            gate.flag.store(true, .release);
            t1.join();
            t2.join();
            try std.testing.expect(a.offset >= 2 and b.offset >= 2);
            const start = @min(a.offset, b.offset);
            var response = try sendFetch(std.testing.allocator, p, 9500, start);
            defer response.deinit(std.testing.allocator);
            try std.testing.expect(response.partitions[0].records.len >= 2);
            var saw_a = false;
            var saw_b = false;
            for (response.partitions[0].records) |record| {
                if (std.mem.eql(u8, record.value.?, "concurrent-a")) saw_a = true;
                if (std.mem.eql(u8, record.value.?, "concurrent-b")) saw_b = true;
            }
            try std.testing.expect(saw_a and saw_b);
        }
    }.run);
}

test "broker allows produce and fetch overlap across clients" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 14;
    try withBrokerProcess(allocator, port, 150, struct {
        fn run(p: u16) !void {
            var gate = StartGate{};
            var prod = ParallelProduceCtx{ .port = p, .gate = &gate, .value = "overlap-value" };
            const t = try std.Thread.spawn(.{}, parallelProduce, .{&prod});
            gate.flag.store(true, .release);
            var response = try sendFetch(std.testing.allocator, p, 9600, 0);
            defer response.deinit(std.testing.allocator);
            t.join();
            try std.testing.expect(prod.offset >= 2);
            var follow = try sendFetch(std.testing.allocator, p, 9601, prod.offset);
            defer follow.deinit(std.testing.allocator);
            try std.testing.expectEqualStrings("overlap-value", follow.partitions[0].records[0].value.?);
        }
    }.run);
}
