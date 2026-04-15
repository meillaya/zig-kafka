const std = @import("std");
const api_versions = @import("../../protocol/api_versions.zig");
const fetch = @import("../../protocol/fetch.zig");
const describe_topic_partitions = @import("../../protocol/describe_topic_partitions.zig");
const header = @import("../../protocol/header.zig");
const primitives = @import("../../protocol/primitives.zig");

pub const supported_apis = [_]api_versions.SupportedApiVersion{
    .{
        .api_key = api_versions.api_key,
        .min_version = api_versions.min_version,
        .max_version = api_versions.max_version,
    },
    .{
        .api_key = describe_topic_partitions.api_key,
        .min_version = describe_topic_partitions.min_version,
        .max_version = describe_topic_partitions.max_version,
    },
    .{
        .api_key = fetch.api_key,
        .min_version = fetch.min_version,
        .max_version = fetch.max_version,
    },
};

pub fn encodePayload(
    allocator: std.mem.Allocator,
    error_code: i16,
    versions: []const api_versions.SupportedApiVersion,
) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendInt(&list, i16, error_code);
    try primitives.appendArrayLen(&list, versions.len);
    for (versions) |entry| {
        try primitives.appendInt(&list, i16, entry.api_key);
        try primitives.appendInt(&list, i16, entry.min_version);
        try primitives.appendInt(&list, i16, entry.max_version);
    }
    try primitives.appendInt(&list, i32, 0);
    return list.toOwnedSlice();
}

pub fn encodeResponse(
    allocator: std.mem.Allocator,
    correlation_id: i32,
    error_code: i16,
) ![]u8 {
    const payload = try encodePayload(
        allocator,
        error_code,
        if (error_code == 0) &supported_apis else &.{},
    );
    defer allocator.free(payload);

    var message = std.array_list.Managed(u8).init(allocator);
    errdefer message.deinit();
    const response_header = header.ResponseHeader{ .correlation_id = correlation_id };
    try response_header.encode(&message);
    try message.appendSlice(payload);
    return message.toOwnedSlice();
}
