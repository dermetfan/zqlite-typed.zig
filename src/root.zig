const std = @import("std");
const root = @import("root");

const utils = @import("utils");
const zqlite = @import("zqlite");

pub const fmt = @import("fmt.zig");

pub const Options = struct {
    log_scope: @TypeOf(.EnumLiteral) = .@"utils/zqlite",
    logErr: fn (err: anyerror, conn: zqlite.Conn, args: anytype) void = logErrDefault,

    fn logErrDefault(err: anyerror, conn: zqlite.Conn, args: anytype) void {
        const sql: []const u8 = args.@"0";
        log.err("{s}: {s}. Statement: {f}", .{ @errorName(err), conn.lastError(), utils.fmt.fmtOneline(sql) });
    }
};

pub const options: Options = if (@hasDecl(root, "zqlite_typed_options")) root.zqlite_typed_options else .{};

pub const log = std.log.scoped(options.log_scope);

pub fn logErr(conn: zqlite.Conn, comptime func_name: std.meta.DeclEnum(zqlite.Conn), args: anytype) (zqlite.Error || error{MultipleStatements})!(blk: {
    const func = @field(zqlite.Conn, @tagName(func_name));
    const func_info = @typeInfo(@TypeOf(func)).@"fn";
    break :blk @typeInfo(func_info.return_type.?).error_union.payload;
}) {
    const func = @field(zqlite.Conn, @tagName(func_name));
    return if (@call(.auto, func, .{conn} ++ args)) |result| result else |err| err: {
        options.logErr(err, conn, args);
        break :err err;
    };
}

pub fn Query(comptime sql_: []const u8, comptime multi: bool, comptime Row_: type, comptime Values_: type) type {
    return struct {
        pub const Row = Row_;
        pub const Column = std.meta.FieldEnum(Row);
        pub const Values = Values_;

        pub const sql = sql_;

        fn column(row: zqlite.Row, comptime col: Column) @TypeOf(row.get(
            columnInfo(@FieldType(Row, @tagName(col))).zqlite_type,
            0, // does not matter here as we don't actually call the function
        )) {
            return row.get(
                columnInfo(@FieldType(Row, @tagName(col))).zqlite_type,
                std.meta.fieldIndex(Row, @tagName(col)).?,
            );
        }

        pub const Rows = if (multi) MultiImpl.Rows;
        pub const queryIterator = if (multi) MultiImpl.queryIterator;
        pub const query = if (multi) MultiImpl.query else SingleImpl.query;

        const MultiImpl = struct {
            fn rows(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !zqlite.Rows {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();

                return logErr(conn, .rows, .{
                    sql,
                    try toZqliteValuesLeaky(Values, arena.allocator(), values),
                });
            }

            pub const Rows = struct {
                zqlite_rows: zqlite.Rows,

                pub fn deinit(self: @This()) void {
                    self.zqlite_rows.deinit();
                }

                pub fn deinitErr(self: @This()) !void {
                    try self.zqlite_rows.deinitErr();
                }

                /// Free the returned row using `freeStructFromRow()`.
                pub fn next(self: *@This(), allocator: std.mem.Allocator) !?Row {
                    if (self.zqlite_rows.next()) |zqlite_row|
                        return try structFromRow(Row, allocator, zqlite_row, column);
                    return null;
                }

                /// Consumes this so `deinit()` or `deinitErr()` no longer have to be called.
                /// Elements in the returned slice need to be freed using `freeStructFromRow()`.
                pub fn toOwnedSlice(self: *@This(), allocator: std.mem.Allocator) ![]Row {
                    errdefer self.deinit();

                    var typed_rows = std.ArrayList(Row).empty;
                    errdefer {
                        for (typed_rows.items) |typed_row| freeStructFromRow(Row, allocator, typed_row);
                        typed_rows.deinit(allocator);
                    }

                    while (try self.next(allocator)) |typed_row|
                        (typed_rows.addOne(allocator) catch |err| {
                            freeStructFromRow(Row, allocator, typed_row);
                            return err;
                        }).* = typed_row;

                    try self.deinitErr();

                    return typed_rows.toOwnedSlice(allocator);
                }
            };

            pub fn queryIterator(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !@This().Rows {
                return .{
                    .zqlite_rows = try @This().rows(allocator, conn, values),
                };
            }

            pub fn query(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) ![]Row {
                var iter = try @This().queryIterator(conn, values);
                return iter.toOwnedSlice(allocator);
            }
        };

        const SingleImpl = struct {
            fn row(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !?zqlite.Row {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();

                return logErr(conn, .row, .{
                    sql,
                    try toZqliteValuesLeaky(Values, arena.allocator(), values),
                });
            }

            pub fn query(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !?Row {
                const zqlite_row = try @This().row(allocator, conn, values) orelse return null;
                errdefer zqlite_row.deinit();

                const typed_row = try structFromRow(Row, allocator, zqlite_row, column);
                errdefer freeStructFromRow(Row, allocator, typed_row);

                try zqlite_row.deinitErr();

                return typed_row;
            }
        };
    };
}

test Query {
    {
        const Q = Query("SELECT a, b, c, d, e, f, g, h, i, j, k, l, m FROM foo", false, struct {
            a: bool,
            b: ?bool,
            c: i64,
            d: ?i64,
            e: f64,
            f: ?f64,
            g: []const u8,
            h: ?[]const u8,
            i: [:0]const u8,
            j: ?[:0]const u8,
            k: zqlite.Blob,
            l: ?zqlite.Blob,
        }, struct {});

        try std.testing.expectEqual(bool, @TypeOf(Q.column(undefined, .a)));
        try std.testing.expectEqual(?bool, @TypeOf(Q.column(undefined, .b)));
        try std.testing.expectEqual(i64, @TypeOf(Q.column(undefined, .c)));
        try std.testing.expectEqual(?i64, @TypeOf(Q.column(undefined, .d)));
        try std.testing.expectEqual(f64, @TypeOf(Q.column(undefined, .e)));
        try std.testing.expectEqual(?f64, @TypeOf(Q.column(undefined, .f)));
        try std.testing.expectEqual([]const u8, @TypeOf(Q.column(undefined, .g)));
        try std.testing.expectEqual(?[]const u8, @TypeOf(Q.column(undefined, .h)));
        try std.testing.expectEqual([:0]const u8, @TypeOf(Q.column(undefined, .i)));
        try std.testing.expectEqual(?[:0]const u8, @TypeOf(Q.column(undefined, .j)));
        try std.testing.expectEqual([]const u8, @TypeOf(Q.column(undefined, .k)));
        try std.testing.expectEqual(?[]const u8, @TypeOf(Q.column(undefined, .l)));
    }

    {
        const Row = struct {};
        const Q = Query("", false, Row, struct {});
        try std.testing.expectEqual(utils.meta.FnErrorUnionPayload(@TypeOf(Q.query)), ?Row);
    }

    {
        const Row = struct {};
        const Q = Query("", true, Row, struct {});
        try std.testing.expectEqual(std.meta.Elem(utils.meta.FnErrorUnionPayload(@TypeOf(Q.query))), Row);
    }
}

pub fn Exec(comptime sql_: []const u8, comptime Values_: type) type {
    const Q = Query(sql_, false, struct {}, Values_);

    const no_args = @typeInfo(Q.Values).@"struct".fields.len == 0;

    return struct {
        pub const Column = Q.Column;
        pub const Values = Q.Values;
        pub const sql = Q.sql;

        pub const execNoArgs = if (no_args) struct {
            pub fn execNoArgs(conn: zqlite.Conn) !void {
                return logErr(conn, .execNoArgs, .{sql});
            }
        }.execNoArgs;

        pub const exec = if (!no_args) struct {
            pub fn exec(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !void {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();

                return logErr(conn, .exec, .{
                    sql,
                    try toZqliteValuesLeaky(Values, arena.allocator(), values),
                });
            }
        }.exec;
    };
}

test Exec {
    {
        const E = Exec("", struct { a: u0 });
        try std.testing.expect(std.meta.hasFn(E, "exec"));
        try std.testing.expect(!std.meta.hasFn(E, "execNoArgs"));
    }

    {
        const E = Exec("", struct {});
        try std.testing.expect(std.meta.hasFn(E, "execNoArgs"));
        try std.testing.expect(!std.meta.hasFn(E, "exec"));
    }
}

pub fn SimpleSelectBy(
    table: []const u8,
    Column: type,
    columns: std.enums.EnumSet(std.meta.FieldEnum(Column)),
    where_equal: std.enums.EnumSet(std.meta.FieldEnum(Column)),
) type {
    var where_alts: [where_equal.count()]@typeInfo(@TypeOf(fmt.fmtIdentifier)).@"fn".return_type.? = undefined;
    var where_equal_iter = where_equal.iterator();
    for (&where_alts) |*where_alt|
        where_alt.* = fmt.fmtIdentifier(@tagName(where_equal_iter.next().?));

    return Query(
        std.fmt.comptimePrint(
            \\SELECT {f}
            \\FROM {f}
            \\WHERE {f}
        , .{
            fmt.fmtIdentifierEnumSet(std.meta.FieldEnum(Column), null, columns, .space),
            fmt.fmtIdentifier(table),
            utils.fmt.fmtJoinSepStr(std.meta.Elem(@TypeOf(where_alts)), "{f} = ?", &where_alts, " AND "),
        }),
        false,
        utils.meta.SubStruct(Column, columns),
        utils.meta.FieldsTuple(utils.meta.SubStruct(Column, where_equal)),
    );
}

test SimpleSelectBy {
    try std.testing.expectEqualStrings(
        \\SELECT "foo", "bar"
        \\FROM "table"
        \\WHERE "foo" = ? AND "bar" = ?
    , SimpleSelectBy("table", struct { foo: void, bar: void }, .full, .full).sql);
}

pub fn SimpleInsert(table: []const u8, Column: type) type {
    return Exec(
        std.fmt.comptimePrint(
            \\INSERT INTO {f} ({f})
            \\VALUES (
        ++ "?, " ** (std.meta.fields(Column).len - 1) ++ "?" ++
            \\)
        , .{
            fmt.fmtIdentifier(table),
            fmt.fmtIdentifierEnumSet(std.meta.FieldEnum(Column), null, .full, .space),
        }),
        utils.meta.FieldsTuple(Column),
    );
}

test SimpleInsert {
    try std.testing.expectEqualStrings(
        \\INSERT INTO "table" ("foo", "bar")
        \\VALUES (?, ?)
    , SimpleInsert("table", struct { foo: void, bar: void }).sql);
}

pub fn SimpleUpsert(table: []const u8, Column: type, update: bool) type {
    return Exec(
        std.fmt.comptimePrint(
            \\INSERT INTO {f} ({f})
            \\VALUES (
        ++ "?, " ** (std.meta.fields(Column).len - 1) ++ "?" ++
            \\)
            \\ON CONFLICT DO
        ++ " " ++ if (!update) "NOTHING" else "UPDATE SET\n" ++ set: {
            const columns = std.meta.fieldNames(Column);
            var sets: [columns.len][]const u8 = undefined;
            for (columns, &sets) |column, *set|
                set.* = std.fmt.comptimePrint("{f} = excluded.{f}", .{
                    fmt.fmtIdentifier(column),
                    fmt.fmtIdentifier(column),
                });
            break :set utils.mem.comptimeJoin(&sets, ",\n");
        }, .{
            fmt.fmtIdentifier(table),
            fmt.fmtIdentifierEnumSet(std.meta.FieldEnum(Column), null, .full, .space),
        }),
        utils.meta.FieldsTuple(Column),
    );
}

test SimpleUpsert {
    try std.testing.expectEqualStrings(
        \\INSERT INTO "table" ("foo", "bar")
        \\VALUES (?, ?)
        \\ON CONFLICT DO NOTHING
    , SimpleUpsert("table", struct { foo: void, bar: void }, false).sql);
    try std.testing.expectEqualStrings(
        \\INSERT INTO "table" ("foo", "bar")
        \\VALUES (?, ?)
        \\ON CONFLICT DO UPDATE SET
        \\"foo" = excluded."foo",
        \\"bar" = excluded."bar"
    , SimpleUpsert("table", struct { foo: void, bar: void }, true).sql);
}

pub fn SimpleDelete(table: []const u8, Values: type) type {
    const values = std.meta.fieldNames(Values);
    var value_alts: [values.len]@typeInfo(@TypeOf(fmt.fmtIdentifier)).@"fn".return_type.? = undefined;
    for (&value_alts, values) |*value_alt, value|
        value_alt.* = fmt.fmtIdentifier(value);

    return Exec(
        std.fmt.comptimePrint(
            \\DELETE FROM {f}
            \\WHERE {f}
        , .{
            fmt.fmtIdentifier(table),
            utils.fmt.fmtJoinSepStr(std.meta.Elem(@TypeOf(value_alts)), "{f} = ?", &value_alts, " AND "),
        }),
        utils.meta.FieldsTuple(Values),
    );
}

test SimpleDelete {
    try std.testing.expectEqualStrings(
        \\DELETE FROM "table"
        \\WHERE "foo" = ? AND "bar" = ?
    , SimpleDelete("table", struct { foo: void, bar: void }).sql);
}

fn columnInfo(Column: type) struct {
    /// Also implies a deinit() function.
    has_from_zqlite: bool,
    has_to_zqlite: bool,
    zqlite_type: type,
} {
    const has_from_zqlite = std.meta.hasFn(Column, "fromZqlite");
    const has_to_zqlite = std.meta.hasFn(Column, "toZqlite");
    const has_deinit = std.meta.hasFn(Column, "deinit");

    if (has_from_zqlite and !has_deinit)
        @compileError(@typeName(Column) ++ ".fromZqlite() implies that a deinit() must exist");

    const from_zqlite_type = if (has_from_zqlite) @typeInfo(@TypeOf(Column.fromZqlite)).@"fn".params[1].type.?;
    const to_zqlite_type = if (has_to_zqlite) utils.meta.FnErrorUnionPayload(@TypeOf(Column.toZqlite));

    if (has_from_zqlite and has_to_zqlite and from_zqlite_type != to_zqlite_type)
        @compileError("Mismatched zqlite types for " ++ @typeName(Column) ++ ".{from,to}Zqlite(): " ++ @typeName(from_zqlite_type) ++ " and " ++ @typeName(to_zqlite_type));

    if (has_from_zqlite and @TypeOf(Column.fromZqlite) != fn (std.mem.Allocator) std.mem.Allocator.Error!from_zqlite_type)
        @compileError(@typeName(Column) ++ ".fromZqlite() has unsupported signature");

    if (has_to_zqlite)
        for ([_]type{
            fn (Column, std.mem.Allocator) std.mem.Allocator.Error!to_zqlite_type,
            fn (*const Column, std.mem.Allocator) std.mem.Allocator.Error!to_zqlite_type,
            fn (*Column, std.mem.Allocator) std.mem.Allocator.Error!to_zqlite_type,
        }) |Fn| {
            if (@TypeOf(Column.toZqlite) == Fn) break;
        } else @compileError(@typeName(Column) ++ ".toZqlite() has unsupported signature");

    return .{
        .has_from_zqlite = has_from_zqlite,
        .has_to_zqlite = has_to_zqlite,
        .zqlite_type = if (has_from_zqlite)
            from_zqlite_type
        else if (has_to_zqlite)
            to_zqlite_type
        else
            Column,
    };
}

fn ZqliteValues(Values: type) type {
    return utils.meta.MapFields(Values, struct {
        fn map(field: std.builtin.Type.StructField) std.builtin.Type.StructField {
            const column_info = columnInfo(field.type);
            return if (column_info.has_to_zqlite)
                .{
                    .name = field.name,
                    .type = column_info.zqlite_type,
                    .default_value_ptr = null,
                    .is_comptime = false,
                    .alignment = null,
                }
            else
                field;
        }
    }.map);
}

/// Calling `toZqlite()` is inherently leaky because we don't know if or how it allocates.
/// We cannot simply `free()` the value it returns because it might be borrowed.
fn toZqliteValuesLeaky(comptime Values: type, arena: std.mem.Allocator, values: Values) !ZqliteValues(Values) {
    var zqlite_values: ZqliteValues(Values) = undefined;
    inline for (std.meta.fields(Values)) |field|
        @field(zqlite_values, field.name) = if (columnInfo(field.type).has_to_zqlite)
            try @field(values, field.name).toZqlite(arena)
        else
            @field(values, field.name);
    return zqlite_values;
}

pub fn structFromRow(
    comptime Row: type,
    allocator: std.mem.Allocator,
    zqlite_row: zqlite.Row,
    column_fn: anytype,
) !Row {
    var row: Row = undefined;

    const columns = comptime std.enums.values(std.meta.FieldEnum(Row));

    var columns_idx: usize = 0;

    errdefer inline for (columns, 0..) |column, comptime_columns_idx| {
        if (comptime_columns_idx == columns_idx) break;

        const Column = @FieldType(Row, @tagName(column));
        const value = @field(row, @tagName(column));

        if (columnInfo(Column).has_from_zqlite)
            value.deinit(allocator)
        else
            free(Column, allocator, value);
    };

    inline for (columns) |column| {
        defer columns_idx += 1;

        const Column = @FieldType(Row, @tagName(column));
        const value = column_fn(zqlite_row, column);

        @field(row, @tagName(column)) = if (columnInfo(Column).has_from_zqlite)
            try Column.fromZqlite(allocator, value)
        else
            try clone(Column, allocator, value);
    }

    return row;
}

pub fn freeStructFromRow(comptime Row: type, allocator: std.mem.Allocator, row: Row) void {
    inline for (@typeInfo(Row).@"struct".fields) |column| {
        const value = @field(row, column.name);

        if (columnInfo(column.type).has_from_zqlite)
            value.deinit(allocator)
        else
            free(column.type, allocator, value);
    }
}

/// Only for types supported by zqlite.
fn clone(comptime T: type, allocator: std.mem.Allocator, value: anytype) std.mem.Allocator.Error!T {
    return switch (@typeInfo(T)) {
        .pointer => |pointer| pointer: {
            const cloned = if (pointer.sentinel()) |sentinel|
                try allocator.dupeSentinel(pointer.child, value, sentinel)
            else
                try allocator.dupe(pointer.child, value);

            break :pointer switch (pointer.size) {
                .slice => cloned,
                .many => cloned.ptr,
                else => |size| @compileError("unsupported pointer size \"" ++ @tagName(size) ++ "\""),
            };
        },

        .optional => |optional| if (value) |v| try clone(optional.child, allocator, v) else null,

        .@"struct" => switch (T) {
            zqlite.Blob => .{ .value = try clone(@FieldType(T, .value), allocator, value) },
            else => @compileError("unsupported struct \"" ++ @typeName(T) ++ "\""),
        },

        .bool, .int, .float => value,

        else => @compileError("unsupported type " ++ @typeName(T)),
    };
}

/// Only for types supported by zqlite.
fn free(comptime T: type, allocator: std.mem.Allocator, value: T) void {
    switch (@typeInfo(T)) {
        .pointer => |pointer| allocator.free(switch (pointer.size) {
            .slice => value,
            .many => if (pointer.sentinel_ptr != null)
                std.mem.span(value)
            else
                @compileError("many-item pointers only supported with sentinel"),
            else => |size| @compileError("unsupported pointer size \"" ++ @tagName(size) ++ "\""),
        }),

        .optional => if (value) |v| free(@TypeOf(v), allocator, v),

        .@"struct" => switch (T) {
            zqlite.Blob => allocator.free(value.value),
            else => @compileError("unsupported struct " ++ @typeName(T)),
        },

        .bool, .int, .float => {},

        else => @compileError("unsupported type " ++ @typeName(T)),
    }
}

test {
    std.testing.refAllDecls(@This());
}
