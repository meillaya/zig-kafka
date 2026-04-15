const client_mod = @import("client.zig");
const fetch_proto = @import("../protocol/fetch.zig");

pub const Consumer = struct {
    client: *client_mod.Client,

    pub fn init(client: *client_mod.Client) Consumer {
        return .{ .client = client };
    }

    pub fn fetch(self: *Consumer, topic: []const u8, partition: i32, offset: i64) !fetch_proto.Response {
        return self.client.fetch(topic, partition, offset);
    }
};
