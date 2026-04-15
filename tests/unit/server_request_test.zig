const std = @import("std");
const kafka = @import("zig-kafka");

const header = kafka.protocol.header;
const server_request = kafka.server.request;

test "server request parses header fields and body via production parser" {
    var payload = std.array_list.Managed(u8).init(std.testing.allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = kafka.protocol.api_versions.api_key,
        .api_version = kafka.protocol.api_versions.max_version,
        .correlation_id = 4242,
        .client_id = "broker-bootstrap-test",
    };
    try req_header.encode(&payload);
    try payload.appendSlice("bootstrap-payload");

    const owned = try std.testing.allocator.dupe(u8, payload.items);
    var parsed = try server_request.parseOwnedFrame(owned);
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(req_header.api_key, parsed.api_key);
    try std.testing.expectEqual(req_header.api_version, parsed.api_version);
    try std.testing.expectEqual(req_header.correlation_id, parsed.correlation_id);
    try std.testing.expectEqualStrings(req_header.client_id, parsed.client_id);
    try std.testing.expectEqualStrings("bootstrap-payload", parsed.body);
}

test "server request parser rejects truncated header" {
    var payload = std.array_list.Managed(u8).init(std.testing.allocator);
    defer payload.deinit();
    const req_header = header.RequestHeader{
        .api_key = kafka.protocol.api_versions.api_key,
        .api_version = 0,
        .correlation_id = 7,
        .client_id = "zig-kafka",
    };
    try req_header.encode(&payload);
    try payload.appendSlice(&.{ 0x01, 0x02, 0x03 });

    const owned = try std.testing.allocator.dupe(u8, payload.items[0..12]);
    defer std.testing.allocator.free(owned);
    try std.testing.expectError(error.UnexpectedEOF, server_request.parseOwnedFrame(owned));
}
