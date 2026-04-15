const std = @import("std");
const kafka = @import("zig-kafka");

const describe = kafka.protocol.describe_topic_partitions;
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

fn sendDescribeUnsupported(allocator: std.mem.Allocator, port: u16) !describe.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    var topics = [_]describe.RequestTopic{.{ .name = "broker-topic-a" }};
    const body = try describe.encodeRequest(allocator, .{
        .topics = topics[0..],
        .response_partition_limit = 10,
        .cursor_topic_name = null,
        .cursor_partition_index = -1,
    });
    defer allocator.free(body);

    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{
        .api_key = describe.api_key,
        .api_version = describe.max_version + 1,
        .correlation_id = 12001,
        .client_id = "err-it",
    };
    try h.encodeVersion(&payload, describe.request_header_version);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);

    var size: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size, size.len);
    const len = std.mem.readInt(i32, &size, .big);
    const buf = try allocator.alloc(u8, @intCast(len));
    defer allocator.free(buf);
    _ = try stream.readAtLeast(buf, buf.len);
    var reader = primitives.Reader.init(buf);
    _ = try header.ResponseHeader.decodeVersion(&reader, describe.response_header_version);
    return try describe.parseResponse(allocator, buf[reader.index..]);
}

fn sendFetchUnsupported(allocator: std.mem.Allocator, port: u16) !fetch.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    const body = try fetch.encodeRequest(allocator, "broker-topic-a", 0, 0, 1024 * 1024);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{
        .api_key = fetch.api_key,
        .api_version = fetch.max_version + 1,
        .correlation_id = 12002,
        .client_id = "err-it",
    };
    try h.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);

    var size: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size, size.len);
    const len = std.mem.readInt(i32, &size, .big);
    const buf = try allocator.alloc(u8, @intCast(len));
    defer allocator.free(buf);
    _ = try stream.readAtLeast(buf, buf.len);
    var reader = primitives.Reader.init(buf);
    _ = try header.ResponseHeader.decode(&reader);
    return try fetch.parseResponse(allocator, buf[reader.index..]);
}

fn sendProduceUnsupported(allocator: std.mem.Allocator, port: u16) !produce.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    const recs = [_]kafka.protocol.record_batch.Record{.{ .value = "x" }};
    const body = try produce.encodeRequest(allocator, "broker-topic-a", 0, &recs, 1000);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{
        .api_key = produce.api_key,
        .api_version = produce.max_version + 1,
        .correlation_id = 12003,
        .client_id = "err-it",
    };
    try h.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    try stream.writeAll(frame.items);

    var size: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size, size.len);
    const len = std.mem.readInt(i32, &size, .big);
    const buf = try allocator.alloc(u8, @intCast(len));
    defer allocator.free(buf);
    _ = try stream.readAtLeast(buf, buf.len);
    var reader = primitives.Reader.init(buf);
    _ = try header.ResponseHeader.decode(&reader);
    return try produce.parseResponse(allocator, buf[reader.index..]);
}

test "broker returns unsupported-version response for DescribeTopicPartitions" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 17;
    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var r = try sendDescribeUnsupported(std.testing.allocator, p);
            defer describe.deinitResponse(std.testing.allocator, &r);
            try std.testing.expectEqual(@as(i16, 35), r.topics[0].error_code);
        }
    }.run);
}

test "broker returns unsupported-version response for Fetch" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 18;
    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var r = try sendFetchUnsupported(std.testing.allocator, p);
            defer r.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 35), r.partitions[0].error_code);
        }
    }.run);
}

test "broker returns unsupported-version response for Produce" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 19;
    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var r = try sendProduceUnsupported(std.testing.allocator, p);
            defer r.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 35), r.partitions[0].error_code);
        }
    }.run);
}
