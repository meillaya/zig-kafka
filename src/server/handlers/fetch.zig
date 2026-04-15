const std = @import("std");
const fetch = @import("../../protocol/fetch.zig");
const record_batch = @import("../../protocol/record_batch.zig");
const error_codes = @import("../../protocol/error_codes.zig");
const storage = @import("../storage.zig");

pub fn handle(allocator: std.mem.Allocator, state: *storage.Storage, request: fetch.Request) !fetch.ResponseBody {
    const topics = try allocator.alloc(fetch.ResponseTopic, request.topics.len);
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |*topic| {
            for (topic.partitions) |partition| allocator.free(partition.records);
            allocator.free(topic.partitions);
        }
        allocator.free(topics);
    }

    for (request.topics, 0..) |topic_req, idx| {
        const partitions = try allocator.alloc(fetch.ResponsePartition, topic_req.partitions.len);
        errdefer allocator.free(partitions);
        for (topic_req.partitions, 0..) |part_req, p_idx| {
            var selection: storage.FetchSelection = undefined;
            var error_code: i16 = @intFromEnum(error_codes.KafkaCode.none);
            if (state.selectRecords(topic_req.topic_name, part_req.partition, part_req.fetch_offset)) |ok| {
                selection = ok;
            } else |err| switch (err) {
                error.UnknownTopicOrPartition => {
                    selection = .{ .records = try allocator.alloc(record_batch.Record, 0), .base_offset = part_req.fetch_offset, .high_watermark = 0 };
                    error_code = @intFromEnum(error_codes.KafkaCode.unknown_topic_or_partition);
                },
                error.OffsetOutOfRange => {
                    selection = .{ .records = try allocator.alloc(record_batch.Record, 0), .base_offset = part_req.fetch_offset, .high_watermark = 0 };
                    error_code = @intFromEnum(error_codes.KafkaCode.offset_out_of_range);
                },
                else => return err,
            }
            partitions[p_idx] = .{
                .partition_index = part_req.partition,
                .error_code = error_code,
                .high_watermark = selection.high_watermark,
                .last_stable_offset = selection.high_watermark,
                .records = selection.records,
                .base_offset = selection.base_offset,
            };
        }
        topics[idx] = .{ .topic_name = topic_req.topic_name, .partitions = partitions };
        topics_filled += 1;
    }

    return .{ .throttle_time_ms = 0, .topics = topics };
}

pub fn deinitResponseBody(allocator: std.mem.Allocator, body: *fetch.ResponseBody) void {
    for (body.topics) |topic| {
        for (topic.partitions) |partition| allocator.free(partition.records);
        allocator.free(topic.partitions);
    }
    allocator.free(body.topics);
    body.* = undefined;
}
