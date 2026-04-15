const std = @import("std");
const primitives = @import("primitives.zig");
const record_batch = @import("record_batch.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 1;
pub const min_version: i16 = 4;
pub const max_version: i16 = 4;

pub const AbortedTransaction = struct {
    producer_id: i64,
    first_offset: i64,
};

pub const PartitionResponse = struct {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    aborted_transactions: []AbortedTransaction,
    records: []record_batch.DecodedRecord,

    pub fn deinit(self: *PartitionResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.aborted_transactions);
        for (self.records) |*record| record.deinit(allocator);
        allocator.free(self.records);
        self.* = undefined;
    }
};

pub const Response = struct {
    throttle_time_ms: i32,
    partitions: []PartitionResponse,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        for (self.partitions) |*partition| partition.deinit(allocator);
        allocator.free(self.partitions);
        self.* = undefined;
    }
};

pub fn encodeRequest(allocator: std.mem.Allocator, topic: []const u8, partition: i32, offset: i64, max_bytes: i32) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendInt(&list, i32, -1); // replica_id
    try primitives.appendInt(&list, i32, 1_000); // max_wait_ms
    try primitives.appendInt(&list, i32, 1); // min_bytes
    try primitives.appendInt(&list, i32, max_bytes);
    try primitives.appendInt(&list, i8, 0); // isolation_level: read uncommitted
    try primitives.appendArrayLen(&list, 1);
    try primitives.appendString(&list, topic);
    try primitives.appendArrayLen(&list, 1);
    try primitives.appendInt(&list, i32, partition);
    try primitives.appendInt(&list, i64, offset);
    try primitives.appendInt(&list, i32, max_bytes);
    return list.toOwnedSlice();
}

pub fn parseResponse(allocator: std.mem.Allocator, bytes: []const u8) !Response {
    var reader = primitives.Reader.init(bytes);
    const throttle_time_ms = try reader.readInt(i32);
    const topic_count = try reader.readInt(i32);
    if (topic_count < 0) return kafka_errors.KafkaError.NegativeLength;
    var partitions = std.array_list.Managed(PartitionResponse).init(allocator);
    errdefer {
        for (partitions.items) |*partition| partition.deinit(allocator);
        partitions.deinit();
    }

    var topic_idx: usize = 0;
    while (topic_idx < @as(usize, @intCast(topic_count))) : (topic_idx += 1) {
        const topic_name = try reader.readString(allocator);
        defer allocator.free(topic_name);
        const partition_count = try reader.readInt(i32);
        if (partition_count < 0) return kafka_errors.KafkaError.NegativeLength;
        var partition_idx: usize = 0;
        while (partition_idx < @as(usize, @intCast(partition_count))) : (partition_idx += 1) {
            const partition_index = try reader.readInt(i32);
            const partition_error = try reader.readInt(i16);
            const high_watermark = try reader.readInt(i64);
            const last_stable_offset = try reader.readInt(i64);
            const aborted_count = try reader.readInt(i32);
            const aborted_transactions = if (aborted_count < 0)
                try allocator.alloc(AbortedTransaction, 0)
            else blk: {
                const txns = try allocator.alloc(AbortedTransaction, @intCast(aborted_count));
                errdefer allocator.free(txns);
                for (txns) |*txn| {
                    txn.* = .{
                        .producer_id = try reader.readInt(i64),
                        .first_offset = try reader.readInt(i64),
                    };
                }
                break :blk txns;
            };
            const records_len = try reader.readInt(i32);
            const records = if (records_len < 0)
                try allocator.dupe(record_batch.DecodedRecord, &.{})
            else blk: {
                const record_bytes = try reader.readBytes(@intCast(records_len));
                break :blk try record_batch.decode(allocator, record_bytes);
            };
            try partitions.append(.{
                .partition_index = partition_index,
                .error_code = partition_error,
                .high_watermark = high_watermark,
                .last_stable_offset = last_stable_offset,
                .aborted_transactions = aborted_transactions,
                .records = records,
            });
        }
    }

    return .{ .throttle_time_ms = throttle_time_ms, .partitions = try partitions.toOwnedSlice() };
}
