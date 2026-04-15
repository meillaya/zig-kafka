const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;

const SupportedApi = api_versions.SupportedApiVersion;

fn encodeApiVersionsResponseFrame(
    allocator: std.mem.Allocator,
    correlation_id: i32,
    supported_apis: []const SupportedApi,
) ![]u8 {
    var payload = std.array_list.Managed(u8).init(allocator);
    defer payload.deinit();

    try primitives.appendInt(&payload, i32, correlation_id);
    try primitives.appendInt(&payload, i16, 0);
    try primitives.appendArrayLen(&payload, supported_apis.len);
    for (supported_apis) |supported_api| {
        try primitives.appendInt(&payload, i16, supported_api.api_key);
        try primitives.appendInt(&payload, i16, supported_api.min_version);
        try primitives.appendInt(&payload, i16, supported_api.max_version);
    }
    try primitives.appendInt(&payload, i32, 0);

    var frame = std.array_list.Managed(u8).init(allocator);
    errdefer frame.deinit();
    try primitives.appendInt(&frame, i32, @intCast(payload.items.len));
    try frame.appendSlice(payload.items);
    return frame.toOwnedSlice();
}

fn findSupportedApi(versions: []const SupportedApi, api_key: i16) ?SupportedApi {
    for (versions) |version| {
        if (version.api_key == api_key) return version;
    }
    return null;
}

test "broker ApiVersions handler preserves correlation id and advertises supported APIs" {
    const allocator = std.testing.allocator;
    const correlation_id: i32 = 5150;
    const advertised = [_]SupportedApi{
        .{
            .api_key = api_versions.api_key,
            .min_version = 0,
            .max_version = api_versions.max_version,
        },
    };

    const frame = try encodeApiVersionsResponseFrame(allocator, correlation_id, &advertised);
    defer allocator.free(frame);

    var frame_reader = primitives.Reader.init(frame);
    const payload_len = try frame_reader.readInt(i32);
    const payload = try frame_reader.readBytes(@intCast(payload_len));
    try std.testing.expectEqual(@as(usize, 0), frame_reader.remaining());

    var payload_reader = primitives.Reader.init(payload);
    const response_header = try header.ResponseHeader.decode(&payload_reader);
    try std.testing.expectEqual(correlation_id, response_header.correlation_id);

    var response = try api_versions.parseResponse(allocator, payload[payload_reader.index..]);
    defer response.deinit(allocator);

    try std.testing.expectEqual(@as(i16, 0), response.error_code);
    try std.testing.expectEqual(@as(i32, 0), response.throttle_time_ms);

    const supported = findSupportedApi(response.api_versions, api_versions.api_key) orelse return error.ApiNotSupported;
    try std.testing.expectEqual(@as(i16, 0), supported.min_version);
    try std.testing.expectEqual(api_versions.max_version, supported.max_version);
}
