const std = @import("std");
const kafka = @import("zig-kafka");

const describe = kafka.protocol.describe_topic_partitions;

const empty_i32 = [_]i32{};

test "describe topic partitions response roundtrip" {
    const allocator = std.testing.allocator;
    const partitions = try allocator.alloc(describe.Partition, 1);
    defer allocator.free(partitions);
    partitions[0] = .{
        .error_code = 0,
        .partition_index = 0,
        .leader_id = 1,
        .leader_epoch = 0,
        .replica_nodes = &.{1},
        .isr_nodes = &.{1},
        .eligible_leader_replicas = &empty_i32,
        .last_known_elr = &empty_i32,
        .offline_replicas = &empty_i32,
    };
    const topics = try allocator.alloc(describe.TopicResponse, 1);
    defer allocator.free(topics);
    topics[0] = .{
        .error_code = 0,
        .name = "broker-topic-a",
        .topic_id = [_]u8{0} ** 16,
        .is_internal = false,
        .partitions = partitions,
        .topic_authorized_operations = 0,
    };
    const response = describe.Response{
        .throttle_time_ms = 0,
        .topics = topics,
        .next_cursor_topic_name = null,
        .next_cursor_partition_index = -1,
    };
    const bytes = try describe.encodeResponse(allocator, response);
    defer allocator.free(bytes);

    var parsed = try describe.parseResponse(allocator, bytes);
    defer describe.deinitResponse(allocator, &parsed);
    try std.testing.expectEqual(@as(usize, 1), parsed.topics.len);
    try std.testing.expectEqualStrings("broker-topic-a", parsed.topics[0].name.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.topics[0].partitions.len);
}
