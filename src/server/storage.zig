const std = @import("std");
const record_batch = @import("../protocol/record_batch.zig");
const catalog = @import("catalog.zig");

pub const StaticRecord = struct {
    value: []const u8,
};

pub const StaticPartition = struct {
    topic_name: []const u8,
    partition_index: i32,
    records: []const StaticRecord,
};

pub const FetchSelection = struct {
    records: []record_batch.Record,
    base_offset: i64,
    high_watermark: i64,
};

const records_topic_a_p0 = [_]StaticRecord{
    .{ .value = "broker-topic-a-p0-r0" },
    .{ .value = "broker-topic-a-p0-r1" },
};
const records_topic_a_p1 = [_]StaticRecord{
    .{ .value = "broker-topic-a-p1-r0" },
};
const records_topic_a_p2 = [_]StaticRecord{};
const records_topic_b_p0 = [_]StaticRecord{
    .{ .value = "broker-topic-b-p0-r0" },
};

pub const static_partitions = [_]StaticPartition{
    .{ .topic_name = "broker-topic-a", .partition_index = 0, .records = &records_topic_a_p0 },
    .{ .topic_name = "broker-topic-a", .partition_index = 1, .records = &records_topic_a_p1 },
    .{ .topic_name = "broker-topic-a", .partition_index = 2, .records = &records_topic_a_p2 },
    .{ .topic_name = "broker-topic-b", .partition_index = 0, .records = &records_topic_b_p0 },
};

pub fn findPartition(topic_name: []const u8, partition_index: i32) ?StaticPartition {
    for (static_partitions) |partition| {
        if (!std.mem.eql(u8, partition.topic_name, topic_name)) continue;
        if (partition.partition_index == partition_index) return partition;
    }
    return null;
}

pub fn selectRecords(
    allocator: std.mem.Allocator,
    topic_name: []const u8,
    partition_index: i32,
    offset: i64,
) !FetchSelection {
    const topic = catalog.findTopic(topic_name) orelse return error.UnknownTopicOrPartition;
    if (partition_index < 0 or partition_index >= topic.partition_count) return error.UnknownTopicOrPartition;
    const partition = findPartition(topic_name, partition_index) orelse return error.UnknownTopicOrPartition;
    const high_watermark: i64 = @intCast(partition.records.len);
    if (offset < 0) return error.OffsetOutOfRange;
    if (offset >= high_watermark) {
        return .{ .records = try allocator.alloc(record_batch.Record, 0), .base_offset = offset, .high_watermark = high_watermark };
    }
    const start: usize = @intCast(offset);
    const out = try allocator.alloc(record_batch.Record, partition.records.len - start);
    errdefer allocator.free(out);
    for (partition.records[start..], 0..) |record, idx| {
        out[idx] = .{ .value = record.value };
    }
    return .{ .records = out, .base_offset = offset, .high_watermark = high_watermark };
}
