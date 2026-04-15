const std = @import("std");
const kafka = @import("zig-kafka");

const fetch = kafka.protocol.fetch;
const handler = kafka.server.fetch_handler;

test "fetch handler returns known and unknown partition results deterministically" {
    const req_topics = [_]fetch.RequestTopic{.{
        .topic_name = try std.testing.allocator.dupe(u8, "broker-topic-a"),
        .partitions = blk: {
            const parts = try std.testing.allocator.alloc(fetch.RequestPartition, 2);
            parts[0] = .{ .partition = 0, .fetch_offset = 0, .max_bytes = 1024 * 1024 };
            parts[1] = .{ .partition = 99, .fetch_offset = 0, .max_bytes = 1024 * 1024 };
            break :blk parts;
        },
    }};
    var request = fetch.Request{
        .replica_id = -1,
        .max_wait_ms = 1000,
        .min_bytes = 1,
        .max_bytes = 1024 * 1024,
        .isolation_level = 0,
        .topics = try std.testing.allocator.dupe(fetch.RequestTopic, &req_topics),
    };
    defer request.deinit(std.testing.allocator);

    var response = try handler.handle(std.testing.allocator, request);
    defer handler.deinitResponseBody(std.testing.allocator, &response);
    try std.testing.expectEqual(@as(usize, 1), response.topics.len);
    try std.testing.expectEqual(@as(usize, 2), response.topics[0].partitions.len);
    try std.testing.expectEqual(@as(i16, 0), response.topics[0].partitions[0].error_code);
    try std.testing.expectEqual(@as(i16, 3), response.topics[0].partitions[1].error_code);
}
