const client_mod = @import("client.zig");
const record_batch = @import("../protocol/record_batch.zig");
const produce = @import("../protocol/produce.zig");

pub const Producer = struct {
    client: *client_mod.Client,

    pub fn init(client: *client_mod.Client) Producer {
        return .{ .client = client };
    }

    pub fn send(self: *Producer, topic: []const u8, partition: i32, records: []const record_batch.Record) !produce.Response {
        return self.client.produce(topic, partition, records);
    }
};
