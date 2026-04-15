const std = @import("std");
const primitives = @import("primitives.zig");
const kafka_errors = @import("../errors.zig");

pub const api_key: i16 = 3;
pub const min_version: i16 = 7;
pub const max_version: i16 = 7;

pub const BrokerInfo = struct {
    node_id: i32,
    host: []u8,
    port: i32,
    rack: ?[]u8,

    pub fn deinit(self: *BrokerInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.host);
        if (self.rack) |rack| allocator.free(rack);
        self.* = undefined;
    }
};

pub const PartitionMetadata = struct {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: []i32,
    isr_nodes: []i32,
    offline_replicas: []i32,

    pub fn deinit(self: *PartitionMetadata, allocator: std.mem.Allocator) void {
        allocator.free(self.replica_nodes);
        allocator.free(self.isr_nodes);
        allocator.free(self.offline_replicas);
        self.* = undefined;
    }
};

pub const TopicMetadata = struct {
    error_code: i16,
    name: []u8,
    is_internal: bool,
    partitions: []PartitionMetadata,

    pub fn deinit(self: *TopicMetadata, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.partitions) |*partition| partition.deinit(allocator);
        allocator.free(self.partitions);
        self.* = undefined;
    }

    pub fn findPartition(self: *const TopicMetadata, partition: i32) ?PartitionMetadata {
        for (self.partitions) |item| {
            if (item.partition_index == partition) return item;
        }
        return null;
    }
};

pub const ClusterMetadata = struct {
    throttle_time_ms: i32,
    brokers: []BrokerInfo,
    cluster_id: ?[]u8,
    controller_id: i32,
    topics: []TopicMetadata,

    pub fn deinit(self: *ClusterMetadata, allocator: std.mem.Allocator) void {
        for (self.brokers) |*broker| broker.deinit(allocator);
        allocator.free(self.brokers);
        if (self.cluster_id) |cluster_id| allocator.free(cluster_id);
        for (self.topics) |*topic| topic.deinit(allocator);
        allocator.free(self.topics);
        self.* = undefined;
    }

    pub fn findBroker(self: *const ClusterMetadata, node_id: i32) ?BrokerInfo {
        for (self.brokers) |broker| {
            if (broker.node_id == node_id) return broker;
        }
        return null;
    }

    pub fn findTopic(self: *const ClusterMetadata, name: []const u8) ?TopicMetadata {
        for (self.topics) |topic| {
            if (std.mem.eql(u8, topic.name, name)) return topic;
        }
        return null;
    }
};

fn readInt32Array(reader: *primitives.Reader, allocator: std.mem.Allocator) ![]i32 {
    const len = try reader.readInt(i32);
    if (len < 0) return kafka_errors.KafkaError.NegativeLength;
    const out = try allocator.alloc(i32, @intCast(len));
    errdefer allocator.free(out);
    for (out) |*item| item.* = try reader.readInt(i32);
    return out;
}

pub fn encodeRequest(allocator: std.mem.Allocator, version: i16, topics: []const []const u8) ![]u8 {
    _ = version;
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendArrayLen(&list, topics.len);
    for (topics) |topic| try primitives.appendString(&list, topic);
    try primitives.appendBool(&list, false);
    return list.toOwnedSlice();
}

pub fn parseResponse(allocator: std.mem.Allocator, bytes: []const u8) !ClusterMetadata {
    var reader = primitives.Reader.init(bytes);
    const throttle_time_ms = try reader.readInt(i32);

    const broker_count_i32 = try reader.readInt(i32);
    if (broker_count_i32 < 0) return kafka_errors.KafkaError.NegativeLength;
    const broker_count: usize = @intCast(broker_count_i32);
    const brokers = try allocator.alloc(BrokerInfo, broker_count);
    var brokers_filled: usize = 0;
    errdefer {
        for (brokers[0..brokers_filled]) |*broker| broker.deinit(allocator);
        allocator.free(brokers);
    }
    while (brokers_filled < broker_count) : (brokers_filled += 1) {
        const node_id = try reader.readInt(i32);
        const host = try reader.readString(allocator);
        errdefer allocator.free(host);
        const port = try reader.readInt(i32);
        const rack = try reader.readNullableString(allocator);
        errdefer if (rack) |owned| allocator.free(owned);
        brokers[brokers_filled] = .{
            .node_id = node_id,
            .host = host,
            .port = port,
            .rack = rack,
        };
    }

    const cluster_id = try reader.readNullableString(allocator);
    errdefer if (cluster_id) |owned| allocator.free(owned);
    const controller_id = try reader.readInt(i32);

    const topic_count_i32 = try reader.readInt(i32);
    if (topic_count_i32 < 0) return kafka_errors.KafkaError.NegativeLength;
    const topic_count: usize = @intCast(topic_count_i32);
    const topics = try allocator.alloc(TopicMetadata, topic_count);
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |*topic| topic.deinit(allocator);
        allocator.free(topics);
    }
    while (topics_filled < topic_count) : (topics_filled += 1) {
        const topic_error_code = try reader.readInt(i16);
        const topic_name = try reader.readString(allocator);
        errdefer allocator.free(topic_name);
        const is_internal = try reader.readBool();
        const partition_count_i32 = try reader.readInt(i32);
        if (partition_count_i32 < 0) return kafka_errors.KafkaError.NegativeLength;
        const partition_count: usize = @intCast(partition_count_i32);
        const partitions = try allocator.alloc(PartitionMetadata, partition_count);
        var partitions_filled: usize = 0;
        errdefer {
            for (partitions[0..partitions_filled]) |*partition| partition.deinit(allocator);
            allocator.free(partitions);
        }
        while (partitions_filled < partition_count) : (partitions_filled += 1) {
            const partition_error = try reader.readInt(i16);
            const partition_id = try reader.readInt(i32);
            const leader_id = try reader.readInt(i32);
            const leader_epoch = try reader.readInt(i32);
            const replica_nodes = try readInt32Array(&reader, allocator);
            errdefer allocator.free(replica_nodes);
            const isr_nodes = try readInt32Array(&reader, allocator);
            errdefer allocator.free(isr_nodes);
            const offline_replicas = try readInt32Array(&reader, allocator);
            errdefer allocator.free(offline_replicas);
            partitions[partitions_filled] = .{
                .error_code = partition_error,
                .partition_index = partition_id,
                .leader_id = leader_id,
                .leader_epoch = leader_epoch,
                .replica_nodes = replica_nodes,
                .isr_nodes = isr_nodes,
                .offline_replicas = offline_replicas,
            };
        }

        topics[topics_filled] = .{
            .error_code = topic_error_code,
            .name = topic_name,
            .is_internal = is_internal,
            .partitions = partitions,
        };
    }

    return .{
        .throttle_time_ms = throttle_time_ms,
        .brokers = brokers,
        .cluster_id = cluster_id,
        .controller_id = controller_id,
        .topics = topics,
    };
}
