const std = @import("std");
const kafka = @import("zig-kafka");

const produce = kafka.protocol.produce;
const handler = kafka.server.produce_handler;
const storage_mod = kafka.server.storage;

fn makeDecoded(value: []const u8) !kafka.protocol.record_batch.DecodedRecord {
    return .{ .offset = 0, .timestamp = 0, .key = null, .value = try std.testing.allocator.dupe(u8, value) };
}

test "produce handler appends records and reports unknown topic" {
    var storage = try storage_mod.Storage.init(std.testing.allocator);
    defer storage.deinit();

    const parts = try std.testing.allocator.alloc(produce.RequestPartition, 2);
    defer {
        for (parts) |*part| {
            for (part.records) |*record| record.deinit(std.testing.allocator);
            std.testing.allocator.free(part.records);
        }
        std.testing.allocator.free(parts);
    }
    parts[0] = .{ .partition = 0, .records = try std.testing.allocator.dupe(kafka.protocol.record_batch.DecodedRecord, &.{try makeDecoded("p0")}) };
    parts[1] = .{ .partition = 0, .records = try std.testing.allocator.dupe(kafka.protocol.record_batch.DecodedRecord, &.{try makeDecoded("ignored")}) };

    const topics = try std.testing.allocator.alloc(produce.RequestTopic, 2);
    defer {
        for (topics) |topic| std.testing.allocator.free(topic.topic_name);
        std.testing.allocator.free(topics);
    }
    topics[0] = .{ .topic_name = try std.testing.allocator.dupe(u8, "broker-topic-a"), .partitions = parts[0..1] };
    topics[1] = .{ .topic_name = try std.testing.allocator.dupe(u8, "missing-topic"), .partitions = parts[1..2] };

    const request = produce.Request{ .transactional_id = null, .acks = 1, .timeout_ms = 1000, .topics = topics };
    var response = try handler.handle(std.testing.allocator, &storage, request);
    defer handler.deinitResponseBody(std.testing.allocator, &response);

    try std.testing.expectEqual(@as(i16, 0), response.topics[0].partitions[0].error_code);
    try std.testing.expectEqual(@as(i16, 3), response.topics[1].partitions[0].error_code);
}
