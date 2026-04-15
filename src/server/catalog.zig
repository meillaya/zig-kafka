const std = @import("std");

pub const StaticTopic = struct {
    name: []const u8,
    partition_count: i32,
};

pub const static_topics = [_]StaticTopic{
    .{ .name = "broker-topic-a", .partition_count = 3 },
    .{ .name = "broker-topic-b", .partition_count = 1 },
};

pub fn findTopic(name: []const u8) ?StaticTopic {
    for (static_topics) |topic| {
        if (std.mem.eql(u8, topic.name, name)) return topic;
    }
    return null;
}
