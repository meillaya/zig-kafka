const std = @import("std");
const kafka = @import("zig-kafka");

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

fn withBrokerProcess(allocator: std.mem.Allocator, port: u16, comptime cb: anytype) !void {
    const cwd = try repoRoot(allocator);
    defer allocator.free(cwd);
    const port_text = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_text);

    const argv = [_][]const u8{
        "zig", "build", "run-broker", "--", "--host", "127.0.0.1", "--port", port_text,
    };

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

fn sendProduce(
    allocator: std.mem.Allocator,
    port: u16,
    correlation_id: i32,
    topic: []const u8,
    partition: i32,
    values: []const []const u8,
) !produce.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    const recs = try allocator.alloc(kafka.protocol.record_batch.Record, values.len);
    defer allocator.free(recs);
    for (values, 0..) |value, idx| recs[idx] = .{ .value = value };
    const body = try produce.encodeRequest(allocator, topic, partition, recs, 1000);
    defer allocator.free(body);

    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = produce.api_key,
        .api_version = produce.max_version,
        .correlation_id = correlation_id,
        .client_id = "produce-it",
    };
    try req_header.encode(&payload);
    try payload.appendSlice(body);

    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);

    var size_buf: [4]u8 = undefined;
    const size_read = try stream.readAtLeast(&size_buf, size_buf.len);
    if (size_read != size_buf.len) return error.UnexpectedEOF;
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    const response_payload = try allocator.alloc(u8, @intCast(payload_len));
    defer allocator.free(response_payload);
    const payload_read = try stream.readAtLeast(response_payload, response_payload.len);
    if (payload_read != response_payload.len) return error.UnexpectedEOF;

    var reader = primitives.Reader.init(response_payload);
    const response_header = try header.ResponseHeader.decode(&reader);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);
    return try produce.parseResponse(allocator, response_payload[reader.index..]);
}

fn sendFetch(
    allocator: std.mem.Allocator,
    port: u16,
    correlation_id: i32,
    topic: []const u8,
    partition: i32,
    offset: i64,
) !fetch.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    const body = try fetch.encodeRequest(allocator, topic, partition, offset, 1024 * 1024);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = fetch.api_key,
        .api_version = fetch.max_version,
        .correlation_id = correlation_id,
        .client_id = "fetch-it",
    };
    try req_header.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);

    var size_buf: [4]u8 = undefined;
    const size_read = try stream.readAtLeast(&size_buf, size_buf.len);
    if (size_read != size_buf.len) return error.UnexpectedEOF;
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    const response_payload = try allocator.alloc(u8, @intCast(payload_len));
    defer allocator.free(response_payload);
    const payload_read = try stream.readAtLeast(response_payload, response_payload.len);
    if (payload_read != response_payload.len) return error.UnexpectedEOF;

    var reader = primitives.Reader.init(response_payload);
    const response_header = try header.ResponseHeader.decode(&reader);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);
    return try fetch.parseResponse(allocator, response_payload[reader.index..]);
}

test "broker produce succeeds for known topic and partition" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 8;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var response = try sendProduce(std.testing.allocator, p, 9001, "broker-topic-a", 0, &.{"new-broker-value"});
            defer response.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 0), response.partitions[0].error_code);
            try std.testing.expectEqual(@as(i64, 2), response.partitions[0].base_offset);
        }
    }.run);
}

test "broker produce and fetch roundtrip through mutable storage" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 9;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var prod = try sendProduce(std.testing.allocator, p, 9002, "broker-topic-a", 0, &.{"roundtrip-value"});
            defer prod.deinit(std.testing.allocator);
            var fetched = try sendFetch(std.testing.allocator, p, 9003, "broker-topic-a", 0, prod.partitions[0].base_offset);
            defer fetched.deinit(std.testing.allocator);
            try std.testing.expectEqualStrings("roundtrip-value", fetched.partitions[0].records[0].value.?);
        }
    }.run);
}

test "broker produce returns deterministic unknown topic error" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 10;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var response = try sendProduce(std.testing.allocator, p, 9004, "missing-topic", 0, &.{"x"});
            defer response.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 3), response.partitions[0].error_code);
        }
    }.run);
}

test "broker produce returns deterministic invalid partition error" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 11;

    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var response = try sendProduce(std.testing.allocator, p, 9005, "broker-topic-a", 99, &.{"x"});
            defer response.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 3), response.partitions[0].error_code);
        }
    }.run);
}
