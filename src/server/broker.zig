const std = @import("std");
const Config = @import("config.zig").Config;
const request = @import("request.zig");
const api_versions = @import("../protocol/api_versions.zig");
const describe = @import("../protocol/describe_topic_partitions.zig");
const fetch = @import("../protocol/fetch.zig");
const produce = @import("../protocol/produce.zig");
const error_codes = @import("../protocol/error_codes.zig");
const api_versions_handler = @import("handlers/api_versions.zig");
const describe_handler = @import("handlers/describe_topic_partitions.zig");
const fetch_handler = @import("handlers/fetch.zig");
const produce_handler = @import("handlers/produce.zig");
const storage = @import("storage.zig");

pub const Broker = struct {
    allocator: std.mem.Allocator,
    config: Config,
    server: std.net.Server,
    storage: storage.Storage,
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(config: Config) !Broker {
        var address = try std.net.Address.parseIp(config.host, config.port);
        const server = try address.listen(.{ .reuse_address = config.reuse_address });
        return .{
            .allocator = config.allocator,
            .config = config,
            .server = server,
            .storage = try storage.Storage.init(config.allocator),
        };
    }

    pub fn deinit(self: *Broker) void {
        self.storage.deinit();
        self.server.deinit();
        self.* = undefined;
    }

    pub fn localPort(self: *const Broker) u16 {
        return self.server.listen_address.getPort();
    }

    pub fn requestStop(self: *Broker) void {
        self.stop_requested.store(true, .release);
        const wake_addr = std.net.Address.parseIp(self.config.host, self.localPort()) catch return;
        var stream = std.net.tcpConnectToAddress(wake_addr) catch return;
        stream.close();
    }

    pub fn serveOnce(self: *Broker) !void {
        var connection = try self.server.accept();
        defer connection.stream.close();
        if (self.stop_requested.load(.acquire)) return;
        try self.handleConnection(&connection.stream);
    }

    pub fn run(self: *Broker) !void {
        while (!self.stop_requested.load(.acquire)) {
            var connection = self.server.accept() catch |err| {
                if (self.stop_requested.load(.acquire)) return;
                return err;
            };
            defer connection.stream.close();
            if (self.stop_requested.load(.acquire)) return;
            self.handleConnection(&connection.stream) catch |err| switch (err) {
                error.UnexpectedEOF => continue,
                else => return err,
            };
        }
    }

    fn wrapResponse(self: *Broker, allocator: std.mem.Allocator, correlation_id: i32, header_version: u8, payload: []const u8) ![]u8 {
        _ = self;
        var message = std.array_list.Managed(u8).init(allocator);
        errdefer message.deinit();
        const response_header = @import("../protocol/header.zig").ResponseHeader{ .correlation_id = correlation_id };
        try response_header.encodeVersion(&message, header_version);
        try message.appendSlice(payload);
        return message.toOwnedSlice();
    }

    fn handleConnection(self: *Broker, stream: *std.net.Stream) !void {
        const frame = try request.readFrame(self.allocator, stream);
        var parsed = try request.parseOwnedFrame(frame);
        defer parsed.deinit(self.allocator);

        switch (parsed.api_key) {
            api_versions.api_key => {
                const error_code: i16 = if (parsed.api_version < api_versions.min_version or parsed.api_version > api_versions.max_version)
                    @intFromEnum(error_codes.KafkaCode.unsupported_version)
                else
                    0;
                const response = try api_versions_handler.encodeResponse(self.allocator, parsed.correlation_id, error_code);
                defer self.allocator.free(response);
                try request.writeFrame(stream, response);
            },
            describe.api_key => {
                if (parsed.api_version < describe.min_version or parsed.api_version > describe.max_version) return;
                var req = try describe.parseRequest(self.allocator, parsed.body);
                defer describe.deinitRequest(self.allocator, &req);
                var resp = try describe_handler.handle(self.allocator, req);
                defer describe.deinitResponse(self.allocator, &resp);
                const response = try describe_handler.encodeResponse(self.allocator, parsed.correlation_id, resp);
                defer self.allocator.free(response);
                try request.writeFrame(stream, response);
            },
            fetch.api_key => {
                if (parsed.api_version < fetch.min_version or parsed.api_version > fetch.max_version) return;
                var req = try fetch.parseRequest(self.allocator, parsed.body);
                defer req.deinit(self.allocator);
                var resp = try fetch_handler.handle(self.allocator, &self.storage, req);
                defer fetch_handler.deinitResponseBody(self.allocator, &resp);
                const payload = try fetch.encodeResponse(self.allocator, resp);
                defer self.allocator.free(payload);
                const response = try self.wrapResponse(self.allocator, parsed.correlation_id, 0, payload);
                defer self.allocator.free(response);
                try request.writeFrame(stream, response);
            },
            produce.api_key => {
                if (parsed.api_version < produce.min_version or parsed.api_version > produce.max_version) return;
                var req = try produce.parseRequest(self.allocator, parsed.body);
                defer req.deinit(self.allocator);
                var resp = try produce_handler.handle(self.allocator, &self.storage, req);
                defer produce_handler.deinitResponseBody(self.allocator, &resp);
                const payload = try produce.encodeResponse(self.allocator, resp);
                defer self.allocator.free(payload);
                const response = try self.wrapResponse(self.allocator, parsed.correlation_id, 0, payload);
                defer self.allocator.free(response);
                try request.writeFrame(stream, response);
            },
            else => return,
        }
    }
};
