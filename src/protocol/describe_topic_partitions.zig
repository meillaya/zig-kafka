const std = @import("std");
const primitives = @import("primitives.zig");

pub const api_key: i16 = 75;
pub const min_version: i16 = 0;
pub const max_version: i16 = 0;
pub const request_header_version: u8 = 2;
pub const response_header_version: u8 = 1;

pub const RequestTopic = struct {
    name: []const u8,
};

pub const Request = struct {
    topics: []RequestTopic,
    response_partition_limit: i32,
    cursor_topic_name: ?[]const u8,
    cursor_partition_index: i32,
};

pub const Partition = struct {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: []const i32,
    isr_nodes: []const i32,
    eligible_leader_replicas: []const i32,
    last_known_elr: []const i32,
    offline_replicas: []const i32,
};

pub const TopicResponse = struct {
    error_code: i16,
    name: ?[]const u8,
    topic_id: [16]u8,
    is_internal: bool,
    partitions: []Partition,
    topic_authorized_operations: i32,
};

pub const Response = struct {
    throttle_time_ms: i32,
    topics: []TopicResponse,
    next_cursor_topic_name: ?[]const u8,
    next_cursor_partition_index: i32,
};

pub fn encodeRequest(allocator: std.mem.Allocator, request: Request) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendCompactArrayLen(&list, request.topics.len);
    for (request.topics) |topic| {
        try primitives.appendCompactString(&list, topic.name);
        try primitives.appendEmptyTaggedFields(&list);
    }
    try primitives.appendInt(&list, i32, request.response_partition_limit);
    try primitives.appendCompactNullableString(&list, request.cursor_topic_name);
    try primitives.appendInt(&list, i32, request.cursor_partition_index);
    try primitives.appendEmptyTaggedFields(&list);
    return list.toOwnedSlice();
}

pub fn parseRequest(allocator: std.mem.Allocator, bytes: []const u8) !Request {
    var reader = primitives.Reader.init(bytes);
    const topic_count = try reader.readCompactArrayLen();
    var topics_list = std.array_list.Managed(RequestTopic).init(allocator);
    defer topics_list.deinit();
    try topics_list.ensureTotalCapacity(topic_count);
    for (0..topic_count) |_| {
        const topic_name = try reader.readCompactStringRef();
        try reader.skipTaggedFields();
        try topics_list.append(.{ .name = topic_name });
    }
    const response_partition_limit = try reader.readInt(i32);
    const cursor_topic_name = try reader.readCompactNullableStringRef();
    const cursor_partition_index = try reader.readInt(i32);
    try reader.skipTaggedFields();
    return .{
        .topics = try topics_list.toOwnedSlice(),
        .response_partition_limit = response_partition_limit,
        .cursor_topic_name = if (cursor_topic_name) |name| try allocator.dupe(u8, name) else null,
        .cursor_partition_index = cursor_partition_index,
    };
}

pub fn deinitRequest(allocator: std.mem.Allocator, request: *Request) void {
    allocator.free(request.topics);
    if (request.cursor_topic_name) |name| allocator.free(name);
    request.* = undefined;
}

fn appendCompactInt32Array(list: *std.array_list.Managed(u8), values: []const i32) !void {
    try primitives.appendCompactArrayLen(list, values.len);
    for (values) |value| try primitives.appendInt(list, i32, value);
}

pub fn encodeResponse(allocator: std.mem.Allocator, response: Response) ![]u8 {
    var list = std.array_list.Managed(u8).init(allocator);
    errdefer list.deinit();
    try primitives.appendInt(&list, i32, response.throttle_time_ms);
    try primitives.appendCompactArrayLen(&list, response.topics.len);
    for (response.topics) |topic| {
        try primitives.appendInt(&list, i16, topic.error_code);
        try primitives.appendCompactNullableString(&list, topic.name);
        try primitives.appendUuid(&list, topic.topic_id);
        try primitives.appendBool(&list, topic.is_internal);
        try primitives.appendCompactArrayLen(&list, topic.partitions.len);
        for (topic.partitions) |partition| {
            try primitives.appendInt(&list, i16, partition.error_code);
            try primitives.appendInt(&list, i32, partition.partition_index);
            try primitives.appendInt(&list, i32, partition.leader_id);
            try primitives.appendInt(&list, i32, partition.leader_epoch);
            try appendCompactInt32Array(&list, partition.replica_nodes);
            try appendCompactInt32Array(&list, partition.isr_nodes);
            try appendCompactInt32Array(&list, partition.eligible_leader_replicas);
            try appendCompactInt32Array(&list, partition.last_known_elr);
            try appendCompactInt32Array(&list, partition.offline_replicas);
            try primitives.appendEmptyTaggedFields(&list);
        }
        try primitives.appendInt(&list, i32, topic.topic_authorized_operations);
        try primitives.appendEmptyTaggedFields(&list);
    }
    try primitives.appendCompactNullableString(&list, response.next_cursor_topic_name);
    try primitives.appendInt(&list, i32, response.next_cursor_partition_index);
    try primitives.appendEmptyTaggedFields(&list);
    return list.toOwnedSlice();
}

fn readCompactInt32Array(allocator: std.mem.Allocator, reader: *primitives.Reader) ![]i32 {
    const count = try reader.readCompactArrayLen();
    const out = try allocator.alloc(i32, count);
    errdefer allocator.free(out);
    for (out) |*item| item.* = try reader.readInt(i32);
    return out;
}

pub fn parseResponse(allocator: std.mem.Allocator, bytes: []const u8) !Response {
    var reader = primitives.Reader.init(bytes);
    const throttle_time_ms = try reader.readInt(i32);
    const topic_count = try reader.readCompactArrayLen();
    const topics = try allocator.alloc(TopicResponse, topic_count);
    var topics_filled: usize = 0;
    errdefer {
        for (topics[0..topics_filled]) |*topic| deinitTopicResponse(allocator, topic);
        allocator.free(topics);
    }
    while (topics_filled < topic_count) : (topics_filled += 1) {
        const error_code = try reader.readInt(i16);
        const name = try reader.readCompactNullableString(allocator);
        errdefer if (name) |owned| allocator.free(owned);
        const topic_id = try reader.readUuid();
        const is_internal = try reader.readBool();
        const partition_count = try reader.readCompactArrayLen();
        const partitions = try allocator.alloc(Partition, partition_count);
        var parts_filled: usize = 0;
        errdefer {
            for (partitions[0..parts_filled]) |*part| deinitPartition(allocator, part);
            allocator.free(partitions);
        }
        while (parts_filled < partition_count) : (parts_filled += 1) {
            const part_error = try reader.readInt(i16);
            const part_index = try reader.readInt(i32);
            const leader_id = try reader.readInt(i32);
            const leader_epoch = try reader.readInt(i32);
            const replica_nodes = try readCompactInt32Array(allocator, &reader);
            errdefer allocator.free(replica_nodes);
            const isr_nodes = try readCompactInt32Array(allocator, &reader);
            errdefer allocator.free(isr_nodes);
            const eligible = try readCompactInt32Array(allocator, &reader);
            errdefer allocator.free(eligible);
            const last_known = try readCompactInt32Array(allocator, &reader);
            errdefer allocator.free(last_known);
            const offline = try readCompactInt32Array(allocator, &reader);
            errdefer allocator.free(offline);
            partitions[parts_filled] = .{
                .error_code = part_error,
                .partition_index = part_index,
                .leader_id = leader_id,
                .leader_epoch = leader_epoch,
                .replica_nodes = replica_nodes,
                .isr_nodes = isr_nodes,
                .eligible_leader_replicas = eligible,
                .last_known_elr = last_known,
                .offline_replicas = offline,
            };
            try reader.skipTaggedFields();
        }
        topics[topics_filled] = .{
            .error_code = error_code,
            .name = name,
            .topic_id = topic_id,
            .is_internal = is_internal,
            .partitions = partitions,
            .topic_authorized_operations = try reader.readInt(i32),
        };
        try reader.skipTaggedFields();
    }
    const next_cursor_topic_name = try reader.readCompactNullableString(allocator);
    errdefer if (next_cursor_topic_name) |owned| allocator.free(owned);
    const next_cursor_partition_index = try reader.readInt(i32);
    try reader.skipTaggedFields();
    return .{
        .throttle_time_ms = throttle_time_ms,
        .topics = topics,
        .next_cursor_topic_name = next_cursor_topic_name,
        .next_cursor_partition_index = next_cursor_partition_index,
    };
}

pub fn deinitPartition(allocator: std.mem.Allocator, partition: *Partition) void {
    allocator.free(partition.replica_nodes);
    allocator.free(partition.isr_nodes);
    allocator.free(partition.eligible_leader_replicas);
    allocator.free(partition.last_known_elr);
    allocator.free(partition.offline_replicas);
    partition.* = undefined;
}

pub fn deinitTopicResponse(allocator: std.mem.Allocator, topic: *TopicResponse) void {
    if (topic.name) |name| allocator.free(name);
    for (topic.partitions) |*partition| deinitPartition(allocator, partition);
    allocator.free(topic.partitions);
    topic.* = undefined;
}

pub fn deinitResponse(allocator: std.mem.Allocator, response: *Response) void {
    for (response.topics) |*topic| deinitTopicResponse(allocator, topic);
    allocator.free(response.topics);
    if (response.next_cursor_topic_name) |name| allocator.free(name);
    response.* = undefined;
}
