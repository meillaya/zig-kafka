pub const errors = @import("errors.zig");
pub const Config = @import("client/config.zig").Config;
pub const Client = @import("client/client.zig").Client;
pub const Producer = @import("client/producer.zig").Producer;
pub const Consumer = @import("client/consumer.zig").Consumer;

pub const protocol = struct {
    pub const primitives = @import("protocol/primitives.zig");
    pub const header = @import("protocol/header.zig");
    pub const api_versions = @import("protocol/api_versions.zig");
    pub const metadata = @import("protocol/metadata.zig");
    pub const produce = @import("protocol/produce.zig");
    pub const fetch = @import("protocol/fetch.zig");
    pub const record_batch = @import("protocol/record_batch.zig");
    pub const error_codes = @import("protocol/error_codes.zig");
};


pub const server = struct {
    pub const Config = @import("server/config.zig").Config;
    pub const Broker = @import("server/broker.zig").Broker;
    pub const request = @import("server/request.zig");
    pub const api_versions_handler = @import("server/handlers/api_versions.zig");
};
