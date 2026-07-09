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

fn formatString(string: []const u8, writer: *std.Io.Writer) !void {
    try writer.writeByte('\'');
    for (string) |char| try switch (char) {
        '\'' => |quote| writer.writeAll(&.{ quote, quote }),
        else => writer.writeByte(char),
    };
    try writer.writeByte('\'');
}

pub fn fmtString(string: []const u8) std.fmt.Alt([]const u8, formatString) {
    return .{ .data = string };
}

test fmtString {
    try std.testing.expectFmt(
        \\'foo'
    , "{f}", .{fmtString("foo")});
    try std.testing.expectFmt(
        \\'fo''o'
    , "{f}", .{fmtString("fo'o")});
    try std.testing.expectFmt(
        \\''
    , "{f}", .{fmtString("")});
}

pub const Pretty = enum {
    minimal,
    space,
    newline,
};

fn FormatEnumSet(E: type, literal: enum { identifier, string }) type {
    return struct {
        qualifier: switch (literal) {
            .identifier => ?[]const u8,
            .string => void,
        } = switch (literal) {
            .identifier => null,
            .string => {},
        },
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

                switch (literal) {
                    .identifier => {
                        if (self.qualifier) |qualifier|
                            try writer.print("{f}.", .{fmtIdentifier(qualifier)});
                        try formatIdentifier(@tagName(e), writer);
                    },
                    .string => try formatString(@tagName(e), writer),
                }
            }
        }
    };
}

pub fn fmtIdentifierEnumSet(comptime E: type, qualifier: ?[]const u8, enum_set: std.enums.EnumSet(E), pretty: Pretty) FormatEnumSet(E, .identifier) {
    return .{
        .qualifier = qualifier,
        .enum_set = enum_set,
        .pretty = pretty,
    };
}

test fmtIdentifierEnumSet {
    const E = enum { foo, bar };

    try std.testing.expectFmt(
        \\"foo","bar"
    , "{f}", .{fmtIdentifierEnumSet(E, null, .full, .minimal)});
    try std.testing.expectFmt(
        \\"foo", "bar"
    , "{f}", .{fmtIdentifierEnumSet(E, null, .full, .space)});
    try std.testing.expectFmt(
        \\"foo",
        \\"bar"
    , "{f}", .{fmtIdentifierEnumSet(E, null, .full, .newline)});

    try std.testing.expectFmt(
        \\"q"."foo","q"."bar"
    , "{f}", .{fmtIdentifierEnumSet(E, "q", .full, .minimal)});
    try std.testing.expectFmt(
        \\"q"."foo", "q"."bar"
    , "{f}", .{fmtIdentifierEnumSet(E, "q", .full, .space)});
    try std.testing.expectFmt(
        \\"q"."foo",
        \\"q"."bar"
    , "{f}", .{fmtIdentifierEnumSet(E, "q", .full, .newline)});

    for (std.enums.values(Pretty)) |pretty| {
        try std.testing.expectFmt("", "{f}", .{fmtIdentifierEnumSet(E, null, .empty, pretty)});
        try std.testing.expectFmt("", "{f}", .{fmtIdentifierEnumSet(E, "q", .empty, pretty)});
    }
}

pub fn fmtStringEnumSet(comptime E: type, enum_set: std.enums.EnumSet(E), pretty: Pretty) FormatEnumSet(E, .string) {
    return .{
        .enum_set = enum_set,
        .pretty = pretty,
    };
}

test fmtStringEnumSet {
    const E = enum { foo, bar };

    try std.testing.expectFmt(
        \\'foo','bar'
    , "{f}", .{fmtStringEnumSet(E, .full, .minimal)});
    try std.testing.expectFmt(
        \\'foo', 'bar'
    , "{f}", .{fmtStringEnumSet(E, .full, .space)});
    try std.testing.expectFmt(
        \\'foo',
        \\'bar'
    , "{f}", .{fmtStringEnumSet(E, .full, .newline)});

    for (std.enums.values(Pretty)) |pretty| {
        try std.testing.expectFmt("", "{f}", .{fmtStringEnumSet(E, .empty, pretty)});
        try std.testing.expectFmt("", "{f}", .{fmtStringEnumSet(E, .empty, pretty)});
    }
}
