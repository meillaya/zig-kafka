const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
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

fn encodeApiReq(allocator: std.mem.Allocator, correlation_id: i32) ![]u8 {
    const body = try api_versions.encodeRequest(allocator);
    defer if (body.len != 0) allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{ .api_key = api_versions.api_key, .api_version = api_versions.max_version, .correlation_id = correlation_id, .client_id = "pipe-it" };
    try h.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}
fn encodeDescribeReq(allocator: std.mem.Allocator, correlation_id: i32, topic_name: []const u8) ![]u8 {
    var topics = [_]describe.RequestTopic{.{ .name = topic_name }};
    const body = try describe.encodeRequest(allocator, .{ .topics = topics[0..], .response_partition_limit = 10, .cursor_topic_name = null, .cursor_partition_index = -1 });
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{ .api_key = describe.api_key, .api_version = describe.max_version, .correlation_id = correlation_id, .client_id = "pipe-it" };
    try h.encodeVersion(&payload, describe.request_header_version);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}
fn encodeProduceReq(allocator: std.mem.Allocator, correlation_id: i32, value: []const u8) ![]u8 {
    const recs = [_]kafka.protocol.record_batch.Record{.{ .value = value }};
    const body = try produce.encodeRequest(allocator, "broker-topic-a", 0, &recs, 1000);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{ .api_key = produce.api_key, .api_version = produce.max_version, .correlation_id = correlation_id, .client_id = "pipe-it" };
    try h.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}
fn encodeFetchReq(allocator: std.mem.Allocator, correlation_id: i32, offset: i64) ![]u8 {
    const body = try fetch.encodeRequest(allocator, "broker-topic-a", 0, offset, 1024 * 1024);
    defer allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const h = header.RequestHeader{ .api_key = fetch.api_key, .api_version = fetch.max_version, .correlation_id = correlation_id, .client_id = "pipe-it" };
    try h.encode(&payload);
    try payload.appendSlice(body);
    var frame = std.array_list.Managed(u8).init(allocator);
    defer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}
fn readFrame(allocator: std.mem.Allocator, stream: *std.net.Stream) ![]u8 {
    var size_buf: [4]u8 = undefined;
    _ = try stream.readAtLeast(&size_buf, size_buf.len);
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    const payload = try allocator.alloc(u8, @intCast(payload_len));
    errdefer allocator.free(payload);
    _ = try stream.readAtLeast(payload, payload.len);
    return payload;
}

test "one connection can serve sequential ApiVersions then DescribeTopicPartitions" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 15;
    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var stream = try std.net.tcpConnectToHost(std.testing.allocator, "127.0.0.1", p);
            defer stream.close();
            const a = try encodeApiReq(std.testing.allocator, 10001);
            defer std.testing.allocator.free(a);
            try stream.writeAll(a);
            const a_resp = try readFrame(std.testing.allocator, &stream);
            defer std.testing.allocator.free(a_resp);
            var a_reader = primitives.Reader.init(a_resp);
            const a_header = try header.ResponseHeader.decode(&a_reader);
            try std.testing.expectEqual(@as(i32, 10001), a_header.correlation_id);
            const d = try encodeDescribeReq(std.testing.allocator, 10002, "broker-topic-a");
            defer std.testing.allocator.free(d);
            try stream.writeAll(d);
            const d_resp = try readFrame(std.testing.allocator, &stream);
            defer std.testing.allocator.free(d_resp);
            var d_reader = primitives.Reader.init(d_resp);
            const d_header = try header.ResponseHeader.decodeVersion(&d_reader, describe.response_header_version);
            try std.testing.expectEqual(@as(i32, 10002), d_header.correlation_id);
            var parsed = try describe.parseResponse(std.testing.allocator, d_resp[d_reader.index..]);
            defer describe.deinitResponse(std.testing.allocator, &parsed);
            try std.testing.expectEqualStrings("broker-topic-a", parsed.topics[0].name.?);
        }
    }.run);
}

test "one connection can pipeline produce then fetch and receive ordered responses" {
    if (!try integrationEnabled()) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const port = try brokerTestPort(allocator) + 16;
    try withBrokerProcess(allocator, port, struct {
        fn run(p: u16) !void {
            var stream = try std.net.tcpConnectToHost(std.testing.allocator, "127.0.0.1", p);
            defer stream.close();
            const prod = try encodeProduceReq(std.testing.allocator, 11001, "one-conn-value");
            defer std.testing.allocator.free(prod);
            const fetch_req = try encodeFetchReq(std.testing.allocator, 11002, 2);
            defer std.testing.allocator.free(fetch_req);
            try stream.writeAll(prod);
            try stream.writeAll(fetch_req);
            const prod_resp = try readFrame(std.testing.allocator, &stream);
            defer std.testing.allocator.free(prod_resp);
            var prod_reader = primitives.Reader.init(prod_resp);
            const prod_header = try header.ResponseHeader.decode(&prod_reader);
            try std.testing.expectEqual(@as(i32, 11001), prod_header.correlation_id);
            var prod_parsed = try produce.parseResponse(std.testing.allocator, prod_resp[prod_reader.index..]);
            defer prod_parsed.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(i16, 0), prod_parsed.partitions[0].error_code);
            const fetch_resp = try readFrame(std.testing.allocator, &stream);
            defer std.testing.allocator.free(fetch_resp);
            var fetch_reader = primitives.Reader.init(fetch_resp);
            const fetch_header = try header.ResponseHeader.decode(&fetch_reader);
            try std.testing.expectEqual(@as(i32, 11002), fetch_header.correlation_id);
            var fetch_parsed = try fetch.parseResponse(std.testing.allocator, fetch_resp[fetch_reader.index..]);
            defer fetch_parsed.deinit(std.testing.allocator);
            try std.testing.expectEqualStrings("one-conn-value", fetch_parsed.partitions[0].records[0].value.?);
        }
    }.run);
}
