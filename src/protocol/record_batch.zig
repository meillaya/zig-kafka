const std = @import("std");
const primitives = @import("primitives.zig");
const kafka_errors = @import("../errors.zig");

pub const Header = struct {
    key: []const u8,
    value: ?[]const u8,
};

pub const Record = struct {
    key: ?[]const u8 = null,
    value: ?[]const u8,
    timestamp_delta: i64 = 0,
    headers: []const Header = &.{},
};

pub const DecodedRecord = struct {
    offset: i64,
    timestamp: i64,
    key: ?[]u8,
    value: ?[]u8,

    pub fn deinit(self: *DecodedRecord, allocator: std.mem.Allocator) void {
        if (self.key) |key| allocator.free(key);
        if (self.value) |value| allocator.free(value);
        self.* = undefined;
    }
};

fn appendRecordPayload(list: *std.array_list.Managed(u8), record: Record, offset_delta: i32) !void {
    var body = std.array_list.Managed(u8).init(list.allocator);
    defer body.deinit();

    try primitives.appendInt(&body, i8, 0); // attributes
    try primitives.writeVarLong(&body, record.timestamp_delta);
    try primitives.writeVarInt(&body, offset_delta);
    if (record.key) |key| {
        try primitives.writeVarInt(&body, @intCast(key.len));
        try body.appendSlice(key);
    } else {
        try primitives.writeVarInt(&body, -1);
    }
    if (record.value) |value| {
        try primitives.writeVarInt(&body, @intCast(value.len));
        try body.appendSlice(value);
    } else {
        try primitives.writeVarInt(&body, -1);
    }
    try primitives.writeVarInt(&body, @intCast(record.headers.len));
    for (record.headers) |header| {
        try primitives.writeVarInt(&body, @intCast(header.key.len));
        try body.appendSlice(header.key);
        if (header.value) |value| {
            try primitives.writeVarInt(&body, @intCast(value.len));
            try body.appendSlice(value);
        } else {
            try primitives.writeVarInt(&body, -1);
        }
    }

    try primitives.writeVarInt(list, @intCast(body.items.len));
    try list.appendSlice(body.items);
}

pub fn encode(allocator: std.mem.Allocator, records: []const Record) ![]u8 {
    if (records.len == 0) return allocator.dupe(u8, &.{});

    var records_bytes = std.array_list.Managed(u8).init(allocator);
    defer records_bytes.deinit();
    for (records, 0..) |record, idx| {
        try appendRecordPayload(&records_bytes, record, @intCast(idx));
    }

    const base_timestamp = std.time.milliTimestamp();
    var inner = std.array_list.Managed(u8).init(allocator);
    defer inner.deinit();
    try primitives.appendInt(&inner, i32, -1); // partition leader epoch
    try primitives.appendInt(&inner, i8, 2); // magic
    try primitives.appendInt(&inner, i32, 0); // CRC placeholder
    try primitives.appendInt(&inner, i16, 0); // attributes
    try primitives.appendInt(&inner, i32, @intCast(records.len - 1));
    try primitives.appendInt(&inner, i64, base_timestamp);
    try primitives.appendInt(&inner, i64, base_timestamp);
    try primitives.appendInt(&inner, i64, -1); // producer id
    try primitives.appendInt(&inner, i16, -1); // producer epoch
    try primitives.appendInt(&inner, i32, -1); // base sequence
    try primitives.appendInt(&inner, i32, @intCast(records.len));
    try inner.appendSlice(records_bytes.items);

    const crc = std.hash.crc.Crc32Iscsi.hash(inner.items[9..]);
    std.mem.writeInt(i32, inner.items[5..9], @bitCast(crc), .big);

    var batch = std.array_list.Managed(u8).init(allocator);
    errdefer batch.deinit();
    try primitives.appendInt(&batch, i64, 0); // base offset ignored by broker on produce
    try primitives.appendInt(&batch, i32, @intCast(inner.items.len));
    try batch.appendSlice(inner.items);
    return batch.toOwnedSlice();
}

pub fn decode(allocator: std.mem.Allocator, bytes: []const u8) ![]DecodedRecord {
    var batches_reader = primitives.Reader.init(bytes);
    var out = std.array_list.Managed(DecodedRecord).init(allocator);
    errdefer {
        for (out.items) |*record| record.deinit(allocator);
        out.deinit();
    }

    while (batches_reader.remaining() > 0) {
        const base_offset = try batches_reader.readInt(i64);
        const batch_length = try batches_reader.readInt(i32);
        if (batch_length < 0) return kafka_errors.KafkaError.NegativeLength;
        const batch_bytes = try batches_reader.readBytes(@intCast(batch_length));
        if (batch_bytes.len < 17) return kafka_errors.KafkaError.InvalidData;
        const crc_expected = std.mem.readInt(i32, batch_bytes[5..9], .big);
        const crc_actual: i32 = @bitCast(std.hash.crc.Crc32Iscsi.hash(batch_bytes[9..]));
        if (crc_expected != crc_actual) return error.InvalidCrc;

        var reader = primitives.Reader.init(batch_bytes);
        _ = try reader.readInt(i32); // leader epoch
        const magic = try reader.readInt(i8);
        if (magic != 2) return kafka_errors.KafkaError.InvalidData;
        _ = try reader.readInt(i32); // crc
        _ = try reader.readInt(i16); // attributes
        _ = try reader.readInt(i32); // last offset delta
        const base_timestamp = try reader.readInt(i64);
        _ = try reader.readInt(i64); // max timestamp
        _ = try reader.readInt(i64); // producer id
        _ = try reader.readInt(i16); // producer epoch
        _ = try reader.readInt(i32); // base sequence
        const record_count = try reader.readInt(i32);
        if (record_count < 0) return kafka_errors.KafkaError.NegativeLength;

        var i: usize = 0;
        while (i < @as(usize, @intCast(record_count))) : (i += 1) {
            const record_length = try primitives.readVarInt(&reader);
            if (record_length < 0) return kafka_errors.KafkaError.NegativeLength;
            const record_bytes = try reader.readBytes(@intCast(record_length));
            var record_reader = primitives.Reader.init(record_bytes);
            _ = try record_reader.readInt(i8); // attrs
            const timestamp_delta = try primitives.readVarLong(&record_reader);
            const offset_delta = try primitives.readVarInt(&record_reader);
            const key_len = try primitives.readVarInt(&record_reader);
            const key = if (key_len < 0)
                null
            else
                try allocator.dupe(u8, try record_reader.readBytes(@intCast(key_len)));
            errdefer if (key) |owned| allocator.free(owned);
            const value_len = try primitives.readVarInt(&record_reader);
            const value = if (value_len < 0)
                null
            else
                try allocator.dupe(u8, try record_reader.readBytes(@intCast(value_len)));
            errdefer if (value) |owned| allocator.free(owned);
            const header_count = try primitives.readVarInt(&record_reader);
            if (header_count < 0) return kafka_errors.KafkaError.NegativeLength;
            var header_idx: usize = 0;
            while (header_idx < @as(usize, @intCast(header_count))) : (header_idx += 1) {
                const key_size = try primitives.readVarInt(&record_reader);
                if (key_size < 0) return kafka_errors.KafkaError.NegativeLength;
                _ = try record_reader.readBytes(@intCast(key_size));
                const value_size = try primitives.readVarInt(&record_reader);
                if (value_size >= 0) _ = try record_reader.readBytes(@intCast(value_size));
            }
            try out.append(.{
                .offset = base_offset + offset_delta,
                .timestamp = base_timestamp + timestamp_delta,
                .key = key,
                .value = value,
            });
        }
    }

    return out.toOwnedSlice();
}
