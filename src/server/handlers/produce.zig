const std = @import("std");
const produce = @import("../../protocol/produce.zig");
const error_codes = @import("../../protocol/error_codes.zig");
const storage = @import("../storage.zig");

pub fn handle(allocator: std.mem.Allocator, state: *storage.Storage, request: produce.Request) !produce.ResponseBody {
    const topics = try allocator.alloc(produce.ResponseTopic, request.topics.len);
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |topic| allocator.free(topic.partitions);
        allocator.free(topics);
    }

    for (request.topics, 0..) |topic_req, idx| {
        const partitions = try allocator.alloc(produce.PartitionResponse, topic_req.partitions.len);
        errdefer allocator.free(partitions);
        for (topic_req.partitions, 0..) |part_req, p_idx| {
            const append_result = state.appendRecords(topic_req.topic_name, part_req.partition, part_req.records) catch |err| switch (err) {
                error.UnknownTopicOrPartition => {
                    partitions[p_idx] = .{
                        .partition = part_req.partition,
                        .error_code = @intFromEnum(error_codes.KafkaCode.unknown_topic_or_partition),
                        .base_offset = -1,
                        .log_append_time_ms = -1,
                        .log_start_offset = 0,
                    };
                    continue;
                },
                else => return err,
            };
            partitions[p_idx] = .{
                .partition = part_req.partition,
                .error_code = @intFromEnum(error_codes.KafkaCode.none),
                .base_offset = append_result.base_offset,
                .log_append_time_ms = 0,
                .log_start_offset = 0,
            };
        }
        topics[idx] = .{ .topic_name = topic_req.topic_name, .partitions = partitions };
        topics_filled += 1;
    }

    return .{ .topics = topics, .throttle_time_ms = 0 };
}

pub fn deinitResponseBody(allocator: std.mem.Allocator, body: *produce.ResponseBody) void {
    for (body.topics) |topic| allocator.free(topic.partitions);
    allocator.free(body.topics);
    body.* = undefined;
}
