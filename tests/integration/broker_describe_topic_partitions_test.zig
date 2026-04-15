const std = @import("std");
const kafka = @import("zig-kafka");

const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;
const describe = kafka.protocol.describe_topic_partitions;

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
        var mutable_stream = stream;
        mutable_stream.close();
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

fn sendDescribe(allocator: std.mem.Allocator, port: u16, correlation_id: i32, topic_name: []const u8) !describe.Response {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    var topics = [_]describe.RequestTopic{.{ .name = topic_name }};
    const body = try describe.encodeRequest(allocator, .{
        .topics = topics[0..],
        .response_partition_limit = 10,
        .cursor_topic_name = null,
        .cursor_partition_index = -1,
    });
    defer allocator.free(body);

    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = describe.api_key,
        .api_version = describe.max_version,
        .correlation_id = correlation_id,
        .client_id = "describe-it",
    };
    try req_header.encodeVersion(&payload, describe.request_header_version);
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
    const response_header = try header.ResponseHeader.decodeVersion(&reader, describe.response_header_version);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);
    return try describe.parseResponse(allocator, response_payload[reader.index..]);
}

test "broker describe topic partitions returns known topic listing" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 2;
    try withBrokerProcess(allocator, port, struct {
        fn run(broker_port: u16) !void {
            var response = try sendDescribe(std.testing.allocator, broker_port, 7001, "broker-topic-a");
            defer describe.deinitResponse(std.testing.allocator, &response);
            try std.testing.expectEqual(@as(usize, 1), response.topics.len);
            try std.testing.expectEqual(@as(i16, 0), response.topics[0].error_code);
            try std.testing.expectEqualStrings("broker-topic-a", response.topics[0].name.?);
            try std.testing.expectEqual(@as(usize, 3), response.topics[0].partitions.len);
        }
    }.run);
}

test "broker describe topic partitions returns deterministic unknown topic error" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 3;
    try withBrokerProcess(allocator, port, struct {
        fn run(broker_port: u16) !void {
            var response = try sendDescribe(std.testing.allocator, broker_port, 7002, "missing-topic");
            defer describe.deinitResponse(std.testing.allocator, &response);
            try std.testing.expectEqual(@as(usize, 1), response.topics.len);
            try std.testing.expectEqual(@as(i16, 3), response.topics[0].error_code);
            try std.testing.expectEqualStrings("missing-topic", response.topics[0].name.?);
            try std.testing.expectEqual(@as(usize, 0), response.topics[0].partitions.len);
        }
    }.run);
}
