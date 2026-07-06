const std = @import("std");

const utils = @import("utils");

fn formatIdentifier(identifier: []const u8, writer: *std.Io.Writer) !void {
    try writer.writeByte('"');
    for (identifier) |char| try switch (char) {
        '"' => |quote| writer.writeAll(&.{ quote, quote }),
        else => writer.writeByte(char),
    };
    try writer.writeByte('"');
}

pub fn fmtIdentifier(identifier: []const u8) std.fmt.Alt([]const u8, formatIdentifier) {
    return .{ .data = identifier };
}

test fmtIdentifier {
    try std.testing.expectFmt(
        \\"foo"
    , "{f}", .{fmtIdentifier("foo")});
    try std.testing.expectFmt(
        \\"fo""o"
    , "{f}", .{fmtIdentifier("fo\"o")});
    try std.testing.expectFmt(
        \\""
    , "{f}", .{fmtIdentifier("")});
}

pub const Pretty = enum {
    minimal,
    space,
    newline,
};

fn FormatEnumSetData(comptime E: type) type {
    return struct {
        qualifier: ?[]const u8 = null,
        enum_set: std.enums.EnumSet(E),
        pretty: Pretty = .space,

        pub fn format(self: @This(), writer: *std.Io.Writer) !void {
            var iter = self.enum_set.iterator();
            var first = true;
            while (iter.next()) |e| {
                if (first)
                    first = false
                else {
                    try writer.writeByte(',');
                    switch (self.pretty) {
                        .minimal => {},
                        .space => try writer.writeByte(' '),
                        .newline => try writer.writeByte('\n'),
                    }
                }

                if (self.qualifier) |qualifier|
                    try writer.print("{f}.", .{fmtIdentifier(qualifier)});
                try formatIdentifier(@tagName(e), writer);
            }
        }
    };
}

pub fn fmtEnumSet(comptime E: type, qualifier: ?[]const u8, enum_set: std.enums.EnumSet(E), pretty: Pretty) FormatEnumSetData(E) {
    return .{
        .qualifier = qualifier,
        .enum_set = enum_set,
        .pretty = pretty,
    };
}

test fmtEnumSet {
    const E = enum { foo, bar };

    try std.testing.expectFmt(
        \\"foo","bar"
    , "{f}", .{fmtEnumSet(E, null, .full, .minimal)});
    try std.testing.expectFmt(
        \\"foo", "bar"
    , "{f}", .{fmtEnumSet(E, null, .full, .space)});
    try std.testing.expectFmt(
        \\"foo",
        \\"bar"
    , "{f}", .{fmtEnumSet(E, null, .full, .newline)});

    try std.testing.expectFmt(
        \\"q"."foo","q"."bar"
    , "{f}", .{fmtEnumSet(E, "q", .full, .minimal)});
    try std.testing.expectFmt(
        \\"q"."foo", "q"."bar"
    , "{f}", .{fmtEnumSet(E, "q", .full, .space)});
    try std.testing.expectFmt(
        \\"q"."foo",
        \\"q"."bar"
    , "{f}", .{fmtEnumSet(E, "q", .full, .newline)});

    for (std.enums.values(Pretty)) |pretty| {
        try std.testing.expectFmt("", "{f}", .{fmtEnumSet(E, null, .empty, pretty)});
        try std.testing.expectFmt("", "{f}", .{fmtEnumSet(E, "q", .empty, pretty)});
    }
}
