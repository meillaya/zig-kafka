const std = @import("std");
const metadata = @import("../protocol/metadata.zig");

pub const MetadataCache = struct {
    allocator: std.mem.Allocator,
    metadata: ?metadata.ClusterMetadata = null,

    pub fn init(allocator: std.mem.Allocator) MetadataCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MetadataCache) void {
        self.clear();
    }

    pub fn clear(self: *MetadataCache) void {
        if (self.metadata) |*m| m.deinit(self.allocator);
        self.metadata = null;
    }

    pub fn replace(self: *MetadataCache, new_metadata: metadata.ClusterMetadata) void {
        self.clear();
        self.metadata = new_metadata;
    }

    pub fn leader(self: *const MetadataCache, topic: []const u8, partition: i32) ?metadata.BrokerInfo {
        const cluster = self.metadata orelse return null;
        const topic_meta = cluster.findTopic(topic) orelse return null;
        const partition_meta = topic_meta.findPartition(partition) orelse return null;
        return cluster.findBroker(partition_meta.leader_id);
    }
};
