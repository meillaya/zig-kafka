const std = @import("std");
const primitives = @import("primitives.zig");
const record_batch = @import("record_batch.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 0;
pub const min_version: i16 = 7;
pub const max_version: i16 = 7;

pub const RequestPartition = struct {
    partition: i32,
    records: []record_batch.DecodedRecord,
};

pub const RequestTopic = struct {
    topic_name: []u8,
    partitions: []RequestPartition,
};

pub const Request = struct {
    transactional_id: ?[]u8,
    acks: i16,
    timeout_ms: i32,
    topics: []RequestTopic,

    pub fn deinit(self: *Request, allocator: std.mem.Allocator) void {
        if (self.transactional_id) |txid| allocator.free(txid);
        for (self.topics) |*topic| {
            allocator.free(topic.topic_name);
            for (topic.partitions) |*partition| {
                for (partition.records) |*record| record.deinit(allocator);
                allocator.free(partition.records);
            }
            allocator.free(topic.partitions);
        }
        allocator.free(self.topics);
        self.* = undefined;
    }
};

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

pub const ResponseTopic = struct {
    topic_name: []const u8,
    partitions: []const PartitionResponse,
};

pub const ResponseBody = struct {
    topics: []const ResponseTopic,
    throttle_time_ms: i32,
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
    try primitives.appendNullableString(&list, null);
    try primitives.appendInt(&list, i16, 1);
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

pub fn parseRequest(allocator: std.mem.Allocator, bytes: []const u8) !Request {
    var reader = primitives.Reader.init(bytes);
    const transactional_id = try reader.readNullableString(allocator);
    errdefer if (transactional_id) |txid| allocator.free(txid);
    const acks = try reader.readInt(i16);
    const timeout_ms = try reader.readInt(i32);
    const topic_count = try reader.readInt(i32);
    if (topic_count < 0) return kafka_errors.KafkaError.NegativeLength;
    const topics = try allocator.alloc(RequestTopic, @intCast(topic_count));
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |*topic| {
            allocator.free(topic.topic_name);
            for (topic.partitions) |*partition| {
                for (partition.records) |*record| record.deinit(allocator);
                allocator.free(partition.records);
            }
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
        var parts_filled: usize = 0;
        errdefer {
            for (partitions[0..parts_filled]) |*partition| {
                for (partition.records) |*record| record.deinit(allocator);
                allocator.free(partition.records);
            }
        }
        while (parts_filled < @as(usize, @intCast(partition_count))) : (parts_filled += 1) {
            const partition = try reader.readInt(i32);
            const records_len = try reader.readInt(i32);
            const records = if (records_len < 0)
                try allocator.dupe(record_batch.DecodedRecord, &.{})
            else blk: {
                const record_bytes = try reader.readBytes(@intCast(records_len));
                break :blk try record_batch.decode(allocator, record_bytes);
            };
            partitions[parts_filled] = .{ .partition = partition, .records = records };
        }
        topics[topics_filled] = .{ .topic_name = topic_name, .partitions = partitions };
    }
    return .{ .transactional_id = transactional_id, .acks = acks, .timeout_ms = timeout_ms, .topics = topics };
}

pub fn encodeResponse(allocator: std.mem.Allocator, body: ResponseBody) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendArrayLen(&list, body.topics.len);
    for (body.topics) |topic| {
        try primitives.appendString(&list, topic.topic_name);
        try primitives.appendArrayLen(&list, topic.partitions.len);
        for (topic.partitions) |partition| {
            try primitives.appendInt(&list, i32, partition.partition);
            try primitives.appendInt(&list, i16, partition.error_code);
            try primitives.appendInt(&list, i64, partition.base_offset);
            try primitives.appendInt(&list, i64, partition.log_append_time_ms);
            try primitives.appendInt(&list, i64, partition.log_start_offset);
        }
    }
    try primitives.appendInt(&list, i32, body.throttle_time_ms);
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
