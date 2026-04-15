const std = @import("std");
const kafka = @import("zig-kafka");

const max_output_bytes = 1024 * 1024;

const HarnessConfig = struct {
    repo_root: []const u8,
    container_name: []const u8,
    bootstrap_server: []const u8,
    topic: []const u8,
    partition: i32,
    fetch_offset: i64,
    manage_broker: bool,

    fn load(allocator: std.mem.Allocator) !HarnessConfig {
        return .{
            .repo_root = try envOrDefault(allocator, "ZIG_KAFKA_REPO_ROOT", "."),
            .container_name = try envOrDefault(allocator, "KAFKA_TEST_CONTAINER_NAME", "zig-kafka-test-broker"),
            .bootstrap_server = try envOrDefault(allocator, "KAFKA_TEST_BOOTSTRAP_SERVER", "127.0.0.1:9092"),
            .topic = try envOrDefault(allocator, "KAFKA_TEST_TOPIC", "zig-kafka-v1-test"),
            .partition = try intEnvOrDefault(i32, allocator, "KAFKA_TEST_PARTITION", 0),
            .fetch_offset = try intEnvOrDefault(i64, allocator, "KAFKA_TEST_FETCH_OFFSET", 0),
            .manage_broker = try boolEnvOrDefault(allocator, "ZIG_KAFKA_MANAGE_BROKER", false),
        };
    }

    fn deinit(self: HarnessConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.repo_root);
        allocator.free(self.container_name);
        allocator.free(self.bootstrap_server);
        allocator.free(self.topic);
    }
};

const Endpoint = struct {
    host: []const u8,
    port: u16,
};

fn envOrDefault(allocator: std.mem.Allocator, name: []const u8, default_value: []const u8) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, default_value),
        else => err,
    };
}

fn intEnvOrDefault(comptime T: type, allocator: std.mem.Allocator, name: []const u8, default_value: T) !T {
    const value = try envOrDefault(allocator, name, "");
    defer allocator.free(value);

    if (value.len == 0) return default_value;
    return std.fmt.parseInt(T, value, 10);
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

fn parseEndpoint(bootstrap_server: []const u8) !Endpoint {
    const separator_index = std.mem.lastIndexOfScalar(u8, bootstrap_server, ':') orelse return error.InvalidBootstrapServer;
    const host = bootstrap_server[0..separator_index];
    const port_text = bootstrap_server[separator_index + 1 ..];
    if (host.len == 0 or port_text.len == 0) return error.InvalidBootstrapServer;

    return .{
        .host = host,
        .port = try std.fmt.parseInt(u16, port_text, 10),
    };
}

fn integrationEnabled() !bool {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    return boolEnvOrDefault(arena.allocator(), "ZIG_KAFKA_RUN_INTEGRATION", false);
}

fn probeBootstrapReachable(allocator: std.mem.Allocator, bootstrap_server: []const u8) !void {
    const endpoint = try parseEndpoint(bootstrap_server);
    var stream = try std.net.tcpConnectToHost(allocator, endpoint.host, endpoint.port);
    defer stream.close();
}

fn assertSuccessfulTermination(term: std.process.Child.Term, stderr: []const u8) !void {
    switch (term) {
        .Exited => |code| if (code == 0) return else {
            std.log.err("child exited with code {d}: {s}", .{ code, stderr });
            return error.ChildProcessFailed;
        },
        else => {
            std.log.err("child terminated unexpectedly: {any}; stderr: {s}", .{ term, stderr });
            return error.ChildProcessFailed;
        },
    }
}

fn runCommandExpectSuccess(allocator: std.mem.Allocator, cwd: []const u8, argv: []const []const u8) !std.process.Child.RunResult {
    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .cwd = cwd,
        .max_output_bytes = max_output_bytes,
    });
    try assertSuccessfulTermination(result.term, result.stderr);
    return result;
}

fn runCommandWithInputExpectSuccess(allocator: std.mem.Allocator, cwd: []const u8, argv: []const []const u8, stdin_bytes: []const u8) !std.process.Child.RunResult {
    var child = std.process.Child.init(argv, allocator);
    child.cwd = cwd;
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();
    {
        const stdin = child.stdin orelse return error.ChildProcessFailed;
        try stdin.writeAll(stdin_bytes);
        stdin.close();
        child.stdin = null;
    }

    var stdout = std.ArrayList(u8).empty;
    var stderr = std.ArrayList(u8).empty;
    try child.collectOutput(allocator, &stdout, &stderr, max_output_bytes);
    const term = try child.wait();
    try assertSuccessfulTermination(term, stderr.items);

    return .{
        .term = term,
        .stdout = try stdout.toOwnedSlice(allocator),
        .stderr = try stderr.toOwnedSlice(allocator),
    };
}

fn freeRunResult(allocator: std.mem.Allocator, result: std.process.Child.RunResult) void {
    allocator.free(result.stdout);
    allocator.free(result.stderr);
}

fn runRepoScript(allocator: std.mem.Allocator, config: HarnessConfig, script_relative_path: []const u8) !void {
    const script_path = try std.fs.path.join(allocator, &.{ config.repo_root, script_relative_path });
    defer allocator.free(script_path);

    const argv = [_][]const u8{script_path};
    const result = try runCommandExpectSuccess(allocator, config.repo_root, &argv);
    defer freeRunResult(allocator, result);
}

fn maybeStartManagedBroker(allocator: std.mem.Allocator, config: HarnessConfig) !void {
    if (!config.manage_broker) return;
    try runRepoScript(allocator, config, "scripts/kafka-test-up.sh");
}

fn maybeStopManagedBroker(allocator: std.mem.Allocator, config: HarnessConfig) void {
    if (!config.manage_broker) return;
    runRepoScript(allocator, config, "scripts/kafka-test-down.sh") catch |err| {
        std.log.warn("failed to stop managed Kafka broker: {s}", .{@errorName(err)});
    };
}

fn brokerApiVersionsOutput(allocator: std.mem.Allocator, config: HarnessConfig) ![]u8 {
    const argv = [_][]const u8{
        "docker",
        "exec",
        config.container_name,
        "/opt/kafka/bin/kafka-broker-api-versions.sh",
        "--bootstrap-server",
        "localhost:9092",
    };
    const result = try runCommandExpectSuccess(allocator, config.repo_root, &argv);
    defer allocator.free(result.stderr);
    return result.stdout;
}

fn topicDescriptionOutput(allocator: std.mem.Allocator, config: HarnessConfig) ![]u8 {
    const argv = [_][]const u8{
        "docker",
        "exec",
        config.container_name,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        config.topic,
    };
    const result = try runCommandExpectSuccess(allocator, config.repo_root, &argv);
    defer allocator.free(result.stderr);
    return result.stdout;
}

fn readLatestOffset(allocator: std.mem.Allocator, config: HarnessConfig, partition_text: []const u8) !i64 {
    const argv = [_][]const u8{
        "docker",
        "exec",
        config.container_name,
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        config.topic,
        "--partitions",
        partition_text,
        "--time",
        "latest",
    };
    const result = try runCommandExpectSuccess(allocator, config.repo_root, &argv);
    defer freeRunResult(allocator, result);

    const trimmed = std.mem.trim(u8, result.stdout, " \r\n\t");
    const separator_index = std.mem.lastIndexOfScalar(u8, trimmed, ':') orelse return error.InvalidOffsetOutput;
    return std.fmt.parseInt(i64, trimmed[separator_index + 1 ..], 10);
}

fn produceRecord(allocator: std.mem.Allocator, config: HarnessConfig, payload: []const u8) !void {
    const argv = [_][]const u8{
        "docker",
        "exec",
        "-i",
        config.container_name,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        config.topic,
        "--sync",
    };
    const input = try std.fmt.allocPrint(allocator, "{s}\n", .{payload});
    defer allocator.free(input);

    const result = try runCommandWithInputExpectSuccess(allocator, config.repo_root, &argv, input);
    defer freeRunResult(allocator, result);
}

fn consumeOne(allocator: std.mem.Allocator, config: HarnessConfig, partition_text: []const u8, offset_text: []const u8) ![]u8 {
    const argv = [_][]const u8{
        "docker",
        "exec",
        config.container_name,
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        config.topic,
        "--partition",
        partition_text,
        "--offset",
        offset_text,
        "--max-messages",
        "1",
        "--timeout-ms",
        "10000",
    };
    const result = try runCommandExpectSuccess(allocator, config.repo_root, &argv);
    defer allocator.free(result.stderr);
    return result.stdout;
}

test "Kafka harness configuration stays well-formed" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();
    const config = try HarnessConfig.load(allocator);
    const endpoint = try parseEndpoint(config.bootstrap_server);

    try std.testing.expect(config.bootstrap_server.len > 0);
    try std.testing.expect(config.container_name.len > 0);
    try std.testing.expect(config.topic.len > 0);
    try std.testing.expect(config.partition >= 0);
    try std.testing.expect(config.fetch_offset >= 0);
    try std.testing.expect(endpoint.host.len > 0);
    try std.testing.expect(endpoint.port > 0);
}

test "Kafka harness broker CLI oracle covers ApiVersions, metadata, and roundtrip" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();
    const config = try HarnessConfig.load(allocator);
    defer config.deinit(allocator);

    try maybeStartManagedBroker(allocator, config);
    defer maybeStopManagedBroker(allocator, config);

    try probeBootstrapReachable(allocator, config.bootstrap_server);

    const partition_text = try std.fmt.allocPrint(allocator, "{d}", .{config.partition});
    defer allocator.free(partition_text);

    const api_versions = try brokerApiVersionsOutput(allocator, config);
    defer allocator.free(api_versions);
    try std.testing.expect(std.mem.containsAtLeast(u8, api_versions, 1, "ApiVersions"));
    try std.testing.expect(std.mem.containsAtLeast(u8, api_versions, 1, "Metadata"));
    try std.testing.expect(std.mem.containsAtLeast(u8, api_versions, 1, "Produce"));
    try std.testing.expect(std.mem.containsAtLeast(u8, api_versions, 1, "Fetch"));

    const topic_description = try topicDescriptionOutput(allocator, config);
    defer allocator.free(topic_description);
    try std.testing.expect(std.mem.containsAtLeast(u8, topic_description, 1, config.topic));
    try std.testing.expect(std.mem.containsAtLeast(u8, topic_description, 1, partition_text));
    try std.testing.expect(std.mem.containsAtLeast(u8, topic_description, 1, "Leader:"));

    const offset_before = try readLatestOffset(allocator, config, partition_text);
    const expected_offset = @max(offset_before, config.fetch_offset);
    const expected_offset_text = try std.fmt.allocPrint(allocator, "{d}", .{expected_offset});
    defer allocator.free(expected_offset_text);

    const payload = try std.fmt.allocPrint(allocator, "zig-kafka-cli-oracle-{d}", .{std.time.milliTimestamp()});
    defer allocator.free(payload);

    try produceRecord(allocator, config, payload);

    const consumed = try consumeOne(allocator, config, partition_text, expected_offset_text);
    defer allocator.free(consumed);

    const normalized = std.mem.trimRight(u8, consumed, "\r\n");
    try std.testing.expectEqualStrings(payload, normalized);
}

test "zig client roundtrip against Kafka broker" {
    if (!try integrationEnabled()) return error.SkipZigTest;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();
    const config = try HarnessConfig.load(allocator);
    defer config.deinit(allocator);

    try maybeStartManagedBroker(allocator, config);
    defer maybeStopManagedBroker(allocator, config);

    try probeBootstrapReachable(allocator, config.bootstrap_server);

    const partition_text = try std.fmt.allocPrint(allocator, "{d}", .{config.partition});
    defer allocator.free(partition_text);
    const starting_offset = try readLatestOffset(allocator, config, partition_text);

    const brokers = [_][]const u8{config.bootstrap_server};
    var client = kafka.Client.init(.{
        .allocator = allocator,
        .bootstrap_brokers = &brokers,
        .client_id = "zig-kafka-integration",
    });
    defer client.deinit();

    const payload = try std.fmt.allocPrint(allocator, "zig-kafka-client-{d}", .{std.time.milliTimestamp()});
    defer allocator.free(payload);

    var producer = kafka.Producer.init(&client);
    var produce_response = try producer.send(config.topic, config.partition, &.{.{ .value = payload }});
    defer produce_response.deinit(allocator);
    try std.testing.expectEqual(starting_offset, produce_response.partitions[0].base_offset);

    var consumer = kafka.Consumer.init(&client);
    var fetch_response = try consumer.fetch(config.topic, config.partition, produce_response.partitions[0].base_offset);
    defer fetch_response.deinit(allocator);

    try std.testing.expect(fetch_response.partitions.len > 0);
    try std.testing.expect(fetch_response.partitions[0].records.len > 0);
    const value = fetch_response.partitions[0].records[0].value orelse return error.InvalidData;
    try std.testing.expectEqualStrings(payload, value);
}
