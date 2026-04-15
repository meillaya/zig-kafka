const std = @import("std");
const record_batch = @import("../protocol/record_batch.zig");
const catalog = @import("catalog.zig");

pub const StoredRecord = struct {
    value: []u8,
};

pub const FetchSelection = struct {
    records: []record_batch.Record,
    base_offset: i64,
    high_watermark: i64,
};

const SeedPartition = struct {
    topic_name: []const u8,
    partition_index: i32,
    records: []const []const u8,
};

const seed_partitions = [_]SeedPartition{
    .{ .topic_name = "broker-topic-a", .partition_index = 0, .records = &.{ "broker-topic-a-p0-r0", "broker-topic-a-p0-r1" } },
    .{ .topic_name = "broker-topic-a", .partition_index = 1, .records = &.{ "broker-topic-a-p1-r0" } },
    .{ .topic_name = "broker-topic-a", .partition_index = 2, .records = &.{} },
    .{ .topic_name = "broker-topic-b", .partition_index = 0, .records = &.{ "broker-topic-b-p0-r0" } },
};

pub const PartitionStore = struct {
    topic_name: []const u8,
    partition_index: i32,
    records: std.array_list.Managed(StoredRecord),

    fn deinit(self: *PartitionStore, allocator: std.mem.Allocator) void {
        for (self.records.items) |record| allocator.free(record.value);
        self.records.deinit();
        self.* = undefined;
    }
};

pub const AppendResult = struct {
    base_offset: i64,
    high_watermark: i64,
};

pub const Storage = struct {
    allocator: std.mem.Allocator,
    partitions: []PartitionStore,

    pub fn init(allocator: std.mem.Allocator) !Storage {
        const partitions = try allocator.alloc(PartitionStore, seed_partitions.len);
        var filled: usize = 0;
        errdefer {
            for (partitions[0..filled]) |*partition| partition.deinit(allocator);
            allocator.free(partitions);
        }
        for (seed_partitions, 0..) |seed, idx| {
            var records = std.array_list.Managed(StoredRecord).init(allocator);
            errdefer records.deinit();
            for (seed.records) |value| {
                try records.append(.{ .value = try allocator.dupe(u8, value) });
            }
            partitions[idx] = .{
                .topic_name = seed.topic_name,
                .partition_index = seed.partition_index,
                .records = records,
            };
            filled += 1;
        }
        return .{ .allocator = allocator, .partitions = partitions };
    }

    pub fn deinit(self: *Storage) void {
        for (self.partitions) |*partition| partition.deinit(self.allocator);
        self.allocator.free(self.partitions);
        self.* = undefined;
    }

    fn findPartitionPtr(self: *Storage, topic_name: []const u8, partition_index: i32) ?*PartitionStore {
        for (self.partitions) |*partition| {
            if (!std.mem.eql(u8, partition.topic_name, topic_name)) continue;
            if (partition.partition_index == partition_index) return partition;
        }
        return null;
    }

    pub fn selectRecords(self: *Storage, topic_name: []const u8, partition_index: i32, offset: i64) !FetchSelection {
        const topic = catalog.findTopic(topic_name) orelse return error.UnknownTopicOrPartition;
        if (partition_index < 0 or partition_index >= topic.partition_count) return error.UnknownTopicOrPartition;
        const partition = self.findPartitionPtr(topic_name, partition_index) orelse return error.UnknownTopicOrPartition;
        const high_watermark: i64 = @intCast(partition.records.items.len);
        if (offset < 0) return error.OffsetOutOfRange;
        if (offset >= high_watermark) {
            return .{ .records = try self.allocator.alloc(record_batch.Record, 0), .base_offset = offset, .high_watermark = high_watermark };
        }
        const start: usize = @intCast(offset);
        const out = try self.allocator.alloc(record_batch.Record, partition.records.items.len - start);
        errdefer self.allocator.free(out);
        for (partition.records.items[start..], 0..) |record, idx| {
            out[idx] = .{ .value = record.value };
        }
        return .{ .records = out, .base_offset = offset, .high_watermark = high_watermark };
    }

    pub fn appendRecords(self: *Storage, topic_name: []const u8, partition_index: i32, records: []const record_batch.DecodedRecord) !AppendResult {
        const topic = catalog.findTopic(topic_name) orelse return error.UnknownTopicOrPartition;
        if (partition_index < 0 or partition_index >= topic.partition_count) return error.UnknownTopicOrPartition;
        const partition = self.findPartitionPtr(topic_name, partition_index) orelse return error.UnknownTopicOrPartition;
        const base_offset: i64 = @intCast(partition.records.items.len);
        for (records) |record| {
            const value = record.value orelse try self.allocator.dupe(u8, "");
            if (record.value == null) {
                try partition.records.append(.{ .value = value });
            } else {
                try partition.records.append(.{ .value = try self.allocator.dupe(u8, value) });
            }
        }
        return .{ .base_offset = base_offset, .high_watermark = @intCast(partition.records.items.len) };
    }
};
