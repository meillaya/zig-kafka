const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
const describe = kafka.protocol.describe_topic_partitions;
const fetch = kafka.protocol.fetch;
const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;

fn envOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}

fn boolEnvOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: bool) !bool {
    const value = try envOrDefault(allocator, name, if (default_value) "1" else "0");
    defer allocator.free(value);

    if (std.ascii.eqlIgnoreCase(value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on")) {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(value, "0") or std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "no") or std.ascii.eqlIgnoreCase(value, "off")) {
        return false;
    }
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

fn encodeApiVersionsRequest(allocator: std.mem.Allocator, correlation_id: i32) ![]u8 {
    const body = try api_versions.encodeRequest(allocator);
    defer if (body.len != 0) allocator.free(body);

    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = api_versions.api_key,
        .api_version = api_versions.max_version,
        .correlation_id = correlation_id,
        .client_id = "broker-integration-test",
    };
    try req_header.encode(&payload);
    try payload.appendSlice(body);

    var frame = std.array_list.Managed(u8).init(allocator);
    errdefer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}

fn encodeUnsupportedApiRequest(allocator: std.mem.Allocator, correlation_id: i32) ![]u8 {
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = 1,
        .api_version = 0,
        .correlation_id = correlation_id,
        .client_id = "broker-integration-test",
    };
    try req_header.encode(&payload);

    var frame = std.array_list.Managed(u8).init(allocator);
    errdefer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}

fn sendApiVersionsProbe(allocator: std.mem.Allocator, host: []const u8, port: u16, correlation_id: i32) ![]u8 {
    var stream = try std.net.tcpConnectToHost(allocator, host, port);
    defer stream.close();

    const request = try encodeApiVersionsRequest(allocator, correlation_id);
    defer allocator.free(request);
    try stream.writeAll(request);

    var size_buf: [4]u8 = undefined;
    const size_read = try stream.readAtLeast(&size_buf, size_buf.len);
    if (size_read != size_buf.len) return error.UnexpectedEOF;
    const payload_len = std.mem.readInt(i32, &size_buf, .big);
    if (payload_len < 4) return error.InvalidData;

    const payload = try allocator.alloc(u8, @intCast(payload_len));
    errdefer allocator.free(payload);
    const payload_read = try stream.readAtLeast(payload, payload.len);
    if (payload_read != payload.len) return error.UnexpectedEOF;
    return payload;
}

test "broker run target answers ApiVersions probes" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const cwd = try repoRoot(allocator);
    defer allocator.free(cwd);

    const port = try brokerTestPort(allocator);
    const port_text = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_text);

    const argv = [_][]const u8{
        "zig",
        "build",
        "run-broker",
        "--",
        "--host",
        "127.0.0.1",
        "--port",
        port_text,
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

    const correlation_id: i32 = 9901;
    const payload = try sendApiVersionsProbe(allocator, "127.0.0.1", port, correlation_id);
    defer allocator.free(payload);

    var reader = primitives.Reader.init(payload);
    const response_header = try header.ResponseHeader.decode(&reader);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);

    var response = try api_versions.parseResponse(allocator, payload[reader.index..]);
    defer response.deinit(allocator);

    try std.testing.expectEqual(@as(i16, 0), response.error_code);
    try std.testing.expect(response.api_versions.len >= 1);

    var saw_api_versions = false;
    var saw_describe = false;
    var saw_fetch = false;
    for (response.api_versions) |supported_api| {
        if (supported_api.api_key == api_versions.api_key) {
            saw_api_versions = true;
            try std.testing.expectEqual(@as(i16, api_versions.min_version), supported_api.min_version);
            try std.testing.expect(supported_api.max_version >= api_versions.max_version);
        }
        if (supported_api.api_key == describe.api_key) {
            saw_describe = true;
            try std.testing.expectEqual(@as(i16, describe.min_version), supported_api.min_version);
            try std.testing.expectEqual(@as(i16, describe.max_version), supported_api.max_version);
        }
        if (supported_api.api_key == fetch.api_key) {
            saw_fetch = true;
            try std.testing.expectEqual(@as(i16, fetch.min_version), supported_api.min_version);
            try std.testing.expectEqual(@as(i16, fetch.max_version), supported_api.max_version);
        }
    }

    try std.testing.expect(saw_api_versions);
    try std.testing.expect(saw_describe);
    try std.testing.expect(saw_fetch);
}

test "broker run target closes connection for unsupported api" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const cwd = try repoRoot(allocator);
    defer allocator.free(cwd);

    const port = try brokerTestPort(allocator) + 1;
    const port_text = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_text);

    const argv = [_][]const u8{
        "zig",
        "build",
        "run-broker",
        "--",
        "--host",
        "127.0.0.1",
        "--port",
        port_text,
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

    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();
    const request = try encodeUnsupportedApiRequest(allocator, 31415);
    defer allocator.free(request);
    try stream.writeAll(request);

    var buf: [1]u8 = undefined;
    const amt = try stream.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), amt);
}
