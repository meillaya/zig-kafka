const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;

fn sendApiVersions(allocator: std.mem.Allocator, port: u16, correlation_id: i32) !void {
    var stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", port);
    defer stream.close();

    const body = try api_versions.encodeRequest(allocator);
    defer if (body.len != 0) allocator.free(body);
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{ .api_key = api_versions.api_key, .api_version = api_versions.max_version, .correlation_id = correlation_id, .client_id = "shutdown-it" };
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
    const resp_header = try header.ResponseHeader.decode(&reader);
    try std.testing.expectEqual(correlation_id, resp_header.correlation_id);
}

const RequestCtx = struct {
    port: u16,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn requestWorker(ctx: *RequestCtx) void {
    sendApiVersions(std.heap.smp_allocator, ctx.port, 12345) catch return;
    ctx.done.store(true, .release);
}

fn brokerMain(broker: *kafka.server.Broker) void {
    broker.run() catch {};
}

test "broker shutdown waits for in-flight delayed request" {
    var broker = try kafka.server.Broker.init(.{ .allocator = std.heap.smp_allocator, .host = "127.0.0.1", .port = 0, .artificial_delay_ms = 200 });
    const broker_thread = try std.Thread.spawn(.{}, brokerMain, .{&broker});
    std.Thread.sleep(100 * std.time.ns_per_ms);

    var ctx = RequestCtx{ .port = broker.localPort() };
    const request_thread = try std.Thread.spawn(.{}, requestWorker, .{&ctx});
    std.Thread.sleep(50 * std.time.ns_per_ms);
    broker.requestStop();
    broker_thread.join();
    request_thread.join();
    defer broker.deinit();
    try std.testing.expect(ctx.done.load(.acquire));
}
