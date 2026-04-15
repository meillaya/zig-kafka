const std = @import("std");
const primitives = @import("primitives.zig");
const record_batch = @import("record_batch.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 0;
pub const min_version: i16 = 7;
pub const max_version: i16 = 7;

pub const PartitionResponse = struct {
    partition: i32,
    error_code: i16,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
};

pub const Response = struct {
    throttle_time_ms: i32,
    partitions: []PartitionResponse,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.partitions);
        self.* = undefined;
    }
};

pub fn encodeRequest(
    allocator: std.mem.Allocator,
    topic: []const u8,
    partition: i32,
    records: []const record_batch.Record,
    timeout_ms: i32,
) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendNullableString(&list, null); // transactional_id
    try primitives.appendInt(&list, i16, 1); // acks=leader ack
    try primitives.appendInt(&list, i32, timeout_ms);
    try primitives.appendArrayLen(&list, 1);
    try primitives.appendString(&list, topic);
    try primitives.appendArrayLen(&list, 1);
    try primitives.appendInt(&list, i32, partition);
    const batch_bytes = try record_batch.encode(allocator, records);
    defer allocator.free(batch_bytes);
    try primitives.appendNullableBytes(&list, batch_bytes);
    return list.toOwnedSlice();
}

pub fn parseResponse(allocator: std.mem.Allocator, bytes: []const u8) !Response {
    var reader = primitives.Reader.init(bytes);
    const topic_count = try reader.readInt(i32);
    if (topic_count < 0) return kafka_errors.KafkaError.NegativeLength;
    var partitions = std.array_list.Managed(PartitionResponse).init(allocator);
    errdefer partitions.deinit();
    var topic_idx: usize = 0;
    while (topic_idx < @as(usize, @intCast(topic_count))) : (topic_idx += 1) {
        const topic_name = try reader.readString(allocator);
        defer allocator.free(topic_name);
        const partition_count = try reader.readInt(i32);
        if (partition_count < 0) return kafka_errors.KafkaError.NegativeLength;
        var partition_idx: usize = 0;
        while (partition_idx < @as(usize, @intCast(partition_count))) : (partition_idx += 1) {
            try partitions.append(.{
                .partition = try reader.readInt(i32),
                .error_code = try reader.readInt(i16),
                .base_offset = try reader.readInt(i64),
                .log_append_time_ms = try reader.readInt(i64),
                .log_start_offset = try reader.readInt(i64),
            });
        }
    }
    const throttle_time_ms = if (reader.remaining() >= 4) try reader.readInt(i32) else 0;
    return .{ .throttle_time_ms = throttle_time_ms, .partitions = try partitions.toOwnedSlice() };
}
