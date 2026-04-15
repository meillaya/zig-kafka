const std = @import("std");
const primitives = @import("primitives.zig");
const record_batch = @import("record_batch.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 1;
pub const min_version: i16 = 4;
pub const max_version: i16 = 4;

pub const RequestPartition = struct {
    partition: i32,
    fetch_offset: i64,
    max_bytes: i32,
};

pub const RequestTopic = struct {
    topic_name: []u8,
    partitions: []RequestPartition,
};

pub const Request = struct {
    replica_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    topics: []RequestTopic,

    pub fn deinit(self: *Request, allocator: std.mem.Allocator) void {
        for (self.topics) |*topic| {
            allocator.free(topic.topic_name);
            allocator.free(topic.partitions);
        }
        allocator.free(self.topics);
        self.* = undefined;
    }
};

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

pub const ResponsePartition = struct {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    records: []const record_batch.Record,
    base_offset: i64,
};

pub const ResponseTopic = struct {
    topic_name: []const u8,
    partitions: []const ResponsePartition,
};

pub const ResponseBody = struct {
    throttle_time_ms: i32,
    topics: []const ResponseTopic,
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

pub fn parseRequest(allocator: std.mem.Allocator, bytes: []const u8) !Request {
    var reader = primitives.Reader.init(bytes);
    const replica_id = try reader.readInt(i32);
    const max_wait_ms = try reader.readInt(i32);
    const min_bytes = try reader.readInt(i32);
    const max_bytes = try reader.readInt(i32);
    const isolation_level = try reader.readInt(i8);
    const topic_count = try reader.readInt(i32);
    if (topic_count < 0) return kafka_errors.KafkaError.NegativeLength;
    const topics = try allocator.alloc(RequestTopic, @intCast(topic_count));
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |*topic| {
            allocator.free(topic.topic_name);
            allocator.free(topic.partitions);
        }
        allocator.free(topics);
    }
    while (topics_filled < @as(usize, @intCast(topic_count))) : (topics_filled += 1) {
        const topic_name = try reader.readString(allocator);
        errdefer allocator.free(topic_name);
        const partition_count = try reader.readInt(i32);
        if (partition_count < 0) return kafka_errors.KafkaError.NegativeLength;
        const partitions = try allocator.alloc(RequestPartition, @intCast(partition_count));
        errdefer allocator.free(partitions);
        for (partitions) |*partition| {
            partition.* = .{
                .partition = try reader.readInt(i32),
                .fetch_offset = try reader.readInt(i64),
                .max_bytes = try reader.readInt(i32),
            };
        }
        topics[topics_filled] = .{ .topic_name = topic_name, .partitions = partitions };
    }
    return .{
        .replica_id = replica_id,
        .max_wait_ms = max_wait_ms,
        .min_bytes = min_bytes,
        .max_bytes = max_bytes,
        .isolation_level = isolation_level,
        .topics = topics,
    };
}

pub fn encodeResponse(allocator: std.mem.Allocator, body: ResponseBody) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendInt(&list, i32, body.throttle_time_ms);
    try primitives.appendArrayLen(&list, body.topics.len);
    for (body.topics) |topic| {
        try primitives.appendString(&list, topic.topic_name);
        try primitives.appendArrayLen(&list, topic.partitions.len);
        for (topic.partitions) |partition| {
            try primitives.appendInt(&list, i32, partition.partition_index);
            try primitives.appendInt(&list, i16, partition.error_code);
            try primitives.appendInt(&list, i64, partition.high_watermark);
            try primitives.appendInt(&list, i64, partition.last_stable_offset);
            try primitives.appendInt(&list, i32, -1); // aborted transactions absent
            const record_bytes = try record_batch.encodeStatic(allocator, partition.base_offset, partition.records);
            defer allocator.free(record_bytes);
            try primitives.appendNullableBytes(&list, record_bytes);
        }
    }
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
