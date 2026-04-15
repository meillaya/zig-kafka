const std = @import("std");
const kafka = @import("zig-kafka");

fn getenvAlloc(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return std.process.getEnvVarOwned(allocator, name) catch error.MissingEnvironment;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const broker = try getenvAlloc(allocator, "KAFKA_BROKER");
    defer allocator.free(broker);
    const topic = try getenvAlloc(allocator, "KAFKA_TOPIC");
    defer allocator.free(topic);
    const partition_text = std.process.getEnvVarOwned(allocator, "KAFKA_PARTITION") catch try allocator.dupe(u8, "0");
    defer allocator.free(partition_text);
    const partition = try std.fmt.parseInt(i32, partition_text, 10);

    const brokers = [_][]const u8{broker};
    var client = kafka.Client.init(.{
        .allocator = allocator,
        .bootstrap_brokers = &brokers,
        .client_id = "zig-kafka-example",
    });
    defer client.deinit();

    const payload = "hello-from-zig-kafka";
    var producer = kafka.Producer.init(&client);
    var produce_response = try producer.send(topic, partition, &.{.{ .value = payload }});
    defer produce_response.deinit(allocator);

    var consumer = kafka.Consumer.init(&client);
    var fetch_response = try consumer.fetch(topic, partition, produce_response.partitions[0].base_offset);
    defer fetch_response.deinit(allocator);

    if (fetch_response.partitions[0].records.len == 0) return error.InvalidData;
    const value = fetch_response.partitions[0].records[0].value orelse return error.InvalidData;
    std.debug.print("produced offset={d} fetched={s}\n", .{ produce_response.partitions[0].base_offset, value });
}
