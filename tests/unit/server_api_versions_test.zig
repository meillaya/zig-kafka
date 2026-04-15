const std = @import("std");
const kafka = @import("zig-kafka");

const api_versions = kafka.protocol.api_versions;
const describe = kafka.protocol.describe_topic_partitions;
const header = kafka.protocol.header;
const primitives = kafka.protocol.primitives;
const handler = kafka.server.api_versions_handler;

test "broker ApiVersions handler preserves correlation id and advertises supported APIs" {
    const allocator = std.testing.allocator;
    const bytes = try handler.encodeResponse(allocator, 5150, 0);
    defer allocator.free(bytes);

    var reader = primitives.Reader.init(bytes);
    const response_header = try header.ResponseHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 5150), response_header.correlation_id);

    var response = try api_versions.parseResponse(allocator, bytes[reader.index..]);
    defer response.deinit(allocator);
    try std.testing.expectEqual(@as(i16, 0), response.error_code);

    var saw_api_versions = false;
    var saw_describe = false;
    for (response.api_versions) |supported_api| {
        if (supported_api.api_key == api_versions.api_key) {
            saw_api_versions = true;
            try std.testing.expectEqual(api_versions.min_version, supported_api.min_version);
            try std.testing.expectEqual(api_versions.max_version, supported_api.max_version);
        }
        if (supported_api.api_key == describe.api_key) {
            saw_describe = true;
            try std.testing.expectEqual(describe.min_version, supported_api.min_version);
            try std.testing.expectEqual(describe.max_version, supported_api.max_version);
        }
    }
    try std.testing.expect(saw_api_versions);
    try std.testing.expect(saw_describe);
}
