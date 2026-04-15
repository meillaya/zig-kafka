const std = @import("std");

pub const ProtocolError = error{
    UnexpectedEOF,
    InvalidData,
    NegativeLength,
};

pub const Reader = struct {
    buf: []const u8,
    index: usize = 0,

    pub fn init(buf: []const u8) Reader {
        return .{ .buf = buf };
    }

    pub fn remaining(self: *const Reader) usize {
        return self.buf.len - self.index;
    }

    pub fn readBytes(self: *Reader, len: usize) ProtocolError![]const u8 {
        if (self.index + len > self.buf.len) return error.UnexpectedEOF;
        defer self.index += len;
        return self.buf[self.index .. self.index + len];
    }

    pub fn readInt(self: *Reader, comptime T: type) ProtocolError!T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .big);
    }

    pub fn readBool(self: *Reader) ProtocolError!bool {
        return (try self.readInt(i8)) != 0;
    }

    pub fn readString(self: *Reader, allocator: std.mem.Allocator) ![]u8 {
        const len = try self.readInt(i16);
        if (len < 0) return error.NegativeLength;
        const bytes = try self.readBytes(@intCast(len));
        return try allocator.dupe(u8, bytes);
    }


    pub fn readStringRef(self: *Reader) ProtocolError![]const u8 {
        const len = try self.readInt(i16);
        if (len < 0) return error.NegativeLength;
        return try self.readBytes(@intCast(len));
    }

    pub fn readNullableString(self: *Reader, allocator: std.mem.Allocator) !?[]u8 {
        const len = try self.readInt(i16);
        if (len < 0) return null;
        const bytes = try self.readBytes(@intCast(len));
        return try allocator.dupe(u8, bytes);
    }

    pub fn readCompactArrayLen(self: *Reader) !usize {
        const value = try readUnsignedVarInt(self);
        if (value == 0) return 0;
        return value - 1;
    }

    pub fn readArray(self: *Reader, comptime T: type, allocator: std.mem.Allocator, reader_fn: fn (*Reader, std.mem.Allocator) anyerror!T) ![]T {
        const len = try self.readInt(i32);
        if (len < 0) return error.NegativeLength;
        const out = try allocator.alloc(T, @intCast(len));
        errdefer allocator.free(out);
        for (out, 0..) |*item, i| {
            _ = i;
            item.* = try reader_fn(self, allocator);
        }
        return out;
    }
};

pub fn appendInt(list: *std.array_list.Managed(u8), comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .big);
    try list.appendSlice(&buf);
}

pub fn appendBool(list: *std.array_list.Managed(u8), value: bool) !void {
    try appendInt(list, i8, if (value) 1 else 0);
}

pub fn appendString(list: *std.array_list.Managed(u8), value: []const u8) !void {
    try appendInt(list, i16, @intCast(value.len));
    try list.appendSlice(value);
}

pub fn appendNullableString(list: *std.array_list.Managed(u8), value: ?[]const u8) !void {
    if (value) |str| {
        try appendString(list, str);
    } else {
        try appendInt(list, i16, -1);
    }
}

pub fn appendNullableBytes(list: *std.array_list.Managed(u8), value: ?[]const u8) !void {
    if (value) |bytes| {
        try appendInt(list, i32, @intCast(bytes.len));
        try list.appendSlice(bytes);
    } else {
        try appendInt(list, i32, -1);
    }
}

pub fn appendArrayLen(list: *std.array_list.Managed(u8), len: usize) !void {
    try appendInt(list, i32, @intCast(len));
}

pub fn writeUnsignedVarInt(list: *std.array_list.Managed(u8), value: u64) !void {
    var v = value;
    while (true) {
        const low: u8 = @intCast(v & 0x7f);
        v >>= 7;
        if (v == 0) {
            try list.append(low);
            return;
        }
        try list.append(low | 0x80);
    }
}

pub fn writeVarInt(list: *std.array_list.Managed(u8), value: i32) !void {
    const encoded: u32 = @bitCast((value << 1) ^ (value >> 31));
    try writeUnsignedVarInt(list, encoded);
}

pub fn writeVarLong(list: *std.array_list.Managed(u8), value: i64) !void {
    const encoded: u64 = @bitCast((value << 1) ^ (value >> 63));
    try writeUnsignedVarInt(list, encoded);
}

pub fn readUnsignedVarInt(reader: *Reader) !usize {
    var shift: u6 = 0;
    var result: u64 = 0;
    while (shift < 64) : (shift += 7) {
        const byte = try reader.readInt(u8);
        result |= (@as(u64, byte & 0x7f) << shift);
        if ((byte & 0x80) == 0) return @intCast(result);
    }
    return error.InvalidData;
}

pub fn readVarInt(reader: *Reader) !i32 {
    const encoded: u32 = @intCast(try readUnsignedVarInt(reader));
    const shifted: i32 = @intCast(encoded >> 1);
    const sign: i32 = -@as(i32, @intCast(encoded & 1));
    return shifted ^ sign;
}

pub fn readVarLong(reader: *Reader) !i64 {
    const encoded: u64 = @intCast(try readUnsignedVarInt(reader));
    const shifted: i64 = @intCast(encoded >> 1);
    const sign: i64 = -@as(i64, @intCast(encoded & 1));
    return shifted ^ sign;
}

test "varint roundtrip" {
    var list = std.array_list.Managed(u8).init(std.testing.allocator);
    defer list.deinit();
    try writeVarInt(&list, -1);
    try writeVarInt(&list, 0);
    try writeVarInt(&list, 123456);

    var reader = Reader.init(list.items);
    try std.testing.expectEqual(@as(i32, -1), try readVarInt(&reader));
    try std.testing.expectEqual(@as(i32, 0), try readVarInt(&reader));
    try std.testing.expectEqual(@as(i32, 123456), try readVarInt(&reader));
}
