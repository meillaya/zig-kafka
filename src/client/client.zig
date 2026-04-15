const std = @import("std");
const Config = @import("config.zig").Config;
const kafka_errors = @import("../errors.zig");
const ConnectionMod = @import("../net/connection.zig");
const Transport = @import("../net/transport.zig").Transport;
const api_versions = @import("../protocol/api_versions.zig");
const metadata = @import("../protocol/metadata.zig");
const produce_proto = @import("../protocol/produce.zig");
const fetch_proto = @import("../protocol/fetch.zig");
const record_batch = @import("../protocol/record_batch.zig");
const MetadataCache = @import("../cluster/metadata_cache.zig").MetadataCache;

pub const NegotiatedVersions = struct {
    metadata: i16,
    produce: i16,
    fetch: i16,
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    config: Config,
    transport: Transport,
    metadata_cache: MetadataCache,
    supported_versions: ?[]api_versions.SupportedApiVersion = null,
    negotiated_versions: ?NegotiatedVersions = null,

    pub fn init(config: Config) Client {
        return .{
            .allocator = config.allocator,
            .config = config,
            .transport = Transport.init(config.allocator, config.client_id),
            .metadata_cache = MetadataCache.init(config.allocator),
        };
    }

    pub fn deinit(self: *Client) void {
        if (self.supported_versions) |versions| self.allocator.free(versions);
        self.metadata_cache.deinit();
        self.* = undefined;
    }

    fn setSupportedVersions(self: *Client, versions: []api_versions.SupportedApiVersion) !void {
        if (self.supported_versions) |existing| self.allocator.free(existing);
        self.supported_versions = versions;
        self.negotiated_versions = .{
            .metadata = try api_versions.selectVersion(versions, metadata.api_key, metadata.min_version, metadata.max_version),
            .produce = try api_versions.selectVersion(versions, produce_proto.api_key, produce_proto.min_version, produce_proto.max_version),
            .fetch = try api_versions.selectVersion(versions, fetch_proto.api_key, fetch_proto.min_version, fetch_proto.max_version),
        };
    }

    fn connect(self: *Client, broker_text: []const u8) !ConnectionMod.Connection {
        const broker = try ConnectionMod.BrokerAddress.parse(broker_text);
        return ConnectionMod.Connection.connect(self.allocator, broker);
    }

    fn sendApiVersions(self: *Client, broker_text: []const u8) !void {
        var connection = try self.connect(broker_text);
        defer connection.close();
        const body = try api_versions.encodeRequest(self.allocator);
        defer if (body.len != 0) self.allocator.free(body);
        const response_bytes = try self.transport.send(&connection, api_versions.api_key, api_versions.max_version, body);
        defer self.allocator.free(response_bytes);
        var response = try api_versions.parseResponse(self.allocator, response_bytes);
        defer response.deinit(self.allocator);
        if (kafka_errors.fromErrorCode(response.error_code)) |err| return err;
        try self.setSupportedVersions(try self.allocator.dupe(api_versions.SupportedApiVersion, response.api_versions));
    }

    fn ensureSupportedVersions(self: *Client) !NegotiatedVersions {
        if (self.negotiated_versions) |versions| return versions;
        for (self.config.bootstrap_brokers) |broker| {
            if (self.sendApiVersions(broker)) {
                return self.negotiated_versions.?;
            } else |_| continue;
        }
        return error.NoBrokersAvailable;
    }

    pub fn refreshMetadata(self: *Client, topics: []const []const u8) !void {
        const versions = try self.ensureSupportedVersions();
        const body = try metadata.encodeRequest(self.allocator, versions.metadata, topics);
        defer self.allocator.free(body);

        for (self.config.bootstrap_brokers) |broker| {
            var connection = self.connect(broker) catch continue;
            defer connection.close();
            const response_bytes = self.transport.send(&connection, metadata.api_key, versions.metadata, body) catch continue;
            defer self.allocator.free(response_bytes);
            var cluster = try metadata.parseResponse(self.allocator, response_bytes);
            if (cluster.topics.len == 0) {
                cluster.deinit(self.allocator);
                continue;
            }
            if (kafka_errors.fromErrorCode(cluster.topics[0].error_code)) |err| {
                cluster.deinit(self.allocator);
                return err;
            }
            self.metadata_cache.replace(cluster);
            return;
        }
        return error.NoBrokersAvailable;
    }

    fn leaderBroker(self: *Client, topic: []const u8, partition: i32) !metadata.BrokerInfo {
        if (self.metadata_cache.leader(topic, partition)) |broker| return broker;
        try self.refreshMetadata(&.{topic});
        return self.metadata_cache.leader(topic, partition) orelse error.NoBrokerForPartition;
    }

    fn withBroker(self: *Client, broker: metadata.BrokerInfo, api_key: i16, api_version: i16, body: []const u8) ![]u8 {
        const address_text = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ broker.host, broker.port });
        defer self.allocator.free(address_text);
        var connection = try self.connect(address_text);
        defer connection.close();
        return self.transport.send(&connection, api_key, api_version, body);
    }

    pub fn produce(self: *Client, topic: []const u8, partition: i32, records: []const record_batch.Record) !produce_proto.Response {
        const versions = try self.ensureSupportedVersions();
        var attempt: usize = 0;
        while (attempt < 2) : (attempt += 1) {
            const leader = try self.leaderBroker(topic, partition);
            const body = try produce_proto.encodeRequest(self.allocator, topic, partition, records, self.config.request_timeout_ms);
            defer self.allocator.free(body);
            const response_bytes = try self.withBroker(leader, produce_proto.api_key, versions.produce, body);
            defer self.allocator.free(response_bytes);
            var response = try produce_proto.parseResponse(self.allocator, response_bytes);
            if (response.partitions.len == 0) {
                response.deinit(self.allocator);
                return error.InvalidData;
            }
            if (kafka_errors.fromErrorCode(response.partitions[0].error_code)) |err| {
                if ((err == error.NotLeaderOrFollower or err == error.LeaderNotAvailable) and attempt == 0) {
                    response.deinit(self.allocator);
                    try self.refreshMetadata(&.{topic});
                    continue;
                }
                response.deinit(self.allocator);
                return err;
            }
            return response;
        }
        return error.BrokerError;
    }

    pub fn fetch(self: *Client, topic: []const u8, partition: i32, offset: i64) !fetch_proto.Response {
        const versions = try self.ensureSupportedVersions();
        var attempt: usize = 0;
        while (attempt < 2) : (attempt += 1) {
            const leader = try self.leaderBroker(topic, partition);
            const body = try fetch_proto.encodeRequest(self.allocator, topic, partition, offset, self.config.fetch_max_bytes);
            defer self.allocator.free(body);
            const response_bytes = try self.withBroker(leader, fetch_proto.api_key, versions.fetch, body);
            defer self.allocator.free(response_bytes);
            var response = try fetch_proto.parseResponse(self.allocator, response_bytes);
            if (response.partitions.len == 0) {
                response.deinit(self.allocator);
                return error.InvalidData;
            }
            if (kafka_errors.fromErrorCode(response.partitions[0].error_code)) |err| {
                if ((err == error.NotLeaderOrFollower or err == error.LeaderNotAvailable) and attempt == 0) {
                    response.deinit(self.allocator);
                    try self.refreshMetadata(&.{topic});
                    continue;
                }
                response.deinit(self.allocator);
                return err;
            }
            return response;
        }
        return error.BrokerError;
    }
};
