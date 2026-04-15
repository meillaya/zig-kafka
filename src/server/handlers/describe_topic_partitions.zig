const std = @import("std");
const describe = @import("../../protocol/describe_topic_partitions.zig");
const header = @import("../../protocol/header.zig");
const catalog = @import("../catalog.zig");
const error_codes = @import("../../protocol/error_codes.zig");

const broker_id: i32 = 1;
const zero_uuid = [_]u8{0} ** 16;

pub fn handle(allocator: std.mem.Allocator, request: describe.Request) !describe.Response {
    const topics = try allocator.alloc(describe.TopicResponse, request.topics.len);
    var topics_filled: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < topics_filled) : (i += 1) describe.deinitTopicResponse(allocator, &topics[i]);
        allocator.free(topics);
    }

    for (request.topics, 0..) |topic_req, idx| {
        if (catalog.findTopic(topic_req.name)) |topic| {
            const partition_count: usize = @intCast(@min(topic.partition_count, request.response_partition_limit));
            const partitions = try allocator.alloc(describe.Partition, partition_count);
            var parts_filled: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < parts_filled) : (i += 1) describe.deinitPartition(allocator, &partitions[i]);
                allocator.free(partitions);
            }
            for (partitions, 0..) |*partition, p_idx| {
                parts_filled += 1;
                const replica_nodes = try allocator.dupe(i32, &[_]i32{broker_id});
                errdefer allocator.free(replica_nodes);
                const isr_nodes = try allocator.dupe(i32, &[_]i32{broker_id});
                errdefer allocator.free(isr_nodes);
                const eligible = try allocator.alloc(i32, 0);
                errdefer allocator.free(eligible);
                const last_known = try allocator.alloc(i32, 0);
                errdefer allocator.free(last_known);
                const offline = try allocator.alloc(i32, 0);
                errdefer allocator.free(offline);
                partition.* = .{
                    .error_code = @intFromEnum(error_codes.KafkaCode.none),
                    .partition_index = @intCast(p_idx),
                    .leader_id = broker_id,
                    .leader_epoch = 0,
                    .replica_nodes = replica_nodes,
                    .isr_nodes = isr_nodes,
                    .eligible_leader_replicas = eligible,
                    .last_known_elr = last_known,
                    .offline_replicas = offline,
                };
            }
            topics[idx] = .{
                .error_code = @intFromEnum(error_codes.KafkaCode.none),
                .name = try allocator.dupe(u8, topic.name),
                .topic_id = zero_uuid,
                .is_internal = false,
                .partitions = partitions,
                .topic_authorized_operations = 0,
            };
            topics_filled += 1;
        } else {
            const partitions = try allocator.alloc(describe.Partition, 0);
            topics[idx] = .{
                .error_code = @intFromEnum(error_codes.KafkaCode.unknown_topic_or_partition),
                .name = try allocator.dupe(u8, topic_req.name),
                .topic_id = zero_uuid,
                .is_internal = false,
                .partitions = partitions,
                .topic_authorized_operations = 0,
            };
            topics_filled += 1;
        }
    }

    return .{
        .throttle_time_ms = 0,
        .topics = topics,
        .next_cursor_topic_name = null,
        .next_cursor_partition_index = -1,
    };
}

pub fn encodeResponse(allocator: std.mem.Allocator, correlation_id: i32, response: describe.Response) ![]u8 {
    const payload = try describe.encodeResponse(allocator, response);
    defer allocator.free(payload);
    var message = std.array_list.Managed(u8).init(allocator);
    errdefer message.deinit();
    const response_header = header.ResponseHeader{ .correlation_id = correlation_id };
    try response_header.encodeVersion(&message, describe.response_header_version);
    try message.appendSlice(payload);
    return message.toOwnedSlice();
}
