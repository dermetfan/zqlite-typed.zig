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

pub fn MergedTables(
    comptime a_qualification: ?[]const u8,
    comptime A: type,
    comptime b_qualification: ?[]const u8,
    comptime B: type,
) type {
    const mapFn = struct {
        fn mapFn(comptime table: []const u8, comptime T: type) fn (utils.meta.FieldInfo(T)) utils.meta.FieldInfo(T) {
            return struct {
                fn map(field: utils.meta.FieldInfo(T)) utils.meta.FieldInfo(T) {
                    var f = field;
                    f.name = table ++ "." ++ f.name;
                    return f;
                }
            }.map;
        }
    }.mapFn;

    return utils.meta.MergedStructs(&[_]type{
        if (a_qualification) |qualification| utils.meta.MapFields(A, mapFn(qualification, A)) else A,
        if (b_qualification) |qualification| utils.meta.MapFields(B, mapFn(qualification, B)) else B,
    });
}

test MergedTables {
    const TableA = struct {
        foo: []const u8,
        bar: zqlite.Blob,
    };
    const TableB = struct {
        foo: []const u8,
        baz: zqlite.Blob,
    };

    {
        const TableMerged = MergedTables("a", TableA, "b", TableB);
        const column_names = std.meta.tags(std.meta.FieldEnum(TableMerged));

        try std.testing.expectEqual(4, column_names.len);
        try std.testing.expectEqual(.@"a.foo", column_names[0]);
        try std.testing.expectEqual(.@"a.bar", column_names[1]);
        try std.testing.expectEqual(.@"b.foo", column_names[2]);
        try std.testing.expectEqual(.@"b.baz", column_names[3]);
    }

    {
        const TableMerged = MergedTables(null, TableA, "b", TableB);
        const column_names = std.meta.tags(std.meta.FieldEnum(TableMerged));

        try std.testing.expectEqual(4, column_names.len);
        try std.testing.expectEqual(.foo, column_names[0]);
        try std.testing.expectEqual(.bar, column_names[1]);
        try std.testing.expectEqual(.@"b.foo", column_names[2]);
        try std.testing.expectEqual(.@"b.baz", column_names[3]);
    }
}

pub fn Query(comptime sql_: []const u8, comptime multi: bool, comptime Row_: type, comptime Values_: type) type {
    return struct {
        pub const Row = Row_;
        pub const Column = std.meta.FieldEnum(Row);
        pub const Values = Values_;

        pub const sql = sql_;

        fn column(result: zqlite.Row, comptime col: Column) GetResult: {
            const Col = @FieldType(Row, @tagName(col));
            break :GetResult @TypeOf(result.get(
                Col,
                0, // does not matter here as we don't actually call the function
            ));
        } {
            const info = std.meta.fieldInfo(Row, col);
            const index = std.meta.fieldIndex(Row, info.name).?;
            return result.get(info.type, index);
        }

        pub const Rows = if (multi) MultiImpl.Rows;
        pub const queryIterator = if (multi) MultiImpl.queryIterator;
        pub const query = if (multi) MultiImpl.query else SingleImpl.query;

        const MultiImpl = struct {
            fn rows(conn: zqlite.Conn, values: Values) !zqlite.Rows {
                return logErr(conn, .rows, .{ sql, values });
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
                    if (self.zqlite_rows.next()) |zqlite_row| {
                        var typed_row: Row = undefined;
                        try structFromRow(allocator, &typed_row, zqlite_row, column);

                        return typed_row;
                    }
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

            pub fn queryIterator(conn: zqlite.Conn, values: Values) !@This().Rows {
                return .{
                    .zqlite_rows = try @This().rows(conn, values),
                };
            }

            pub fn query(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) ![]Row {
                var iter = try @This().queryIterator(conn, values);
                return iter.toOwnedSlice(allocator);
            }
        };

        const SingleImpl = struct {
            fn row(conn: zqlite.Conn, values: Values) !?zqlite.Row {
                return logErr(conn, .row, .{ sql, values });
            }

            pub fn query(allocator: std.mem.Allocator, conn: zqlite.Conn, values: Values) !?Row {
                const zqlite_row = try @This().row(conn, values) orelse return null;
                errdefer zqlite_row.deinit();

                var typed_row: Row = undefined;
                try structFromRow(allocator, &typed_row, zqlite_row, column);
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
            pub fn exec(conn: zqlite.Conn, values: Values) !void {
                return logErr(conn, .exec, .{ sql, values });
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

pub fn SimpleInsert(table: []const u8, Column: type) type {
    return Exec(
        std.fmt.comptimePrint(
            \\INSERT INTO {f} ({f})
            \\VALUES (
        ++ "?, " ** (std.meta.fields(Column).len - 1) ++ "?" ++
            \\)
        , .{
            fmt.fmtIdentifier(table),
            fmt.fmtEnumSet(std.meta.FieldEnum(Column), null, .full, .space),
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
            fmt.fmtEnumSet(std.meta.FieldEnum(Column), null, .full, .space),
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

pub fn structFromRow(
    allocator: std.mem.Allocator,
    target_ptr: anytype,
    row: zqlite.Row,
    column_fn: anytype,
) !void {
    const Target = std.meta.Child(@TypeOf(target_ptr));
    const target: *Target = @ptrCast(target_ptr);

    const fields = comptime std.enums.values(std.meta.FieldEnum(Target));

    var clone_ctx = struct {
        allocator: std.mem.Allocator,

        allocated_mem: [fields.len][]const u8 = undefined,
        allocated: usize = 0,
        allocated_mem_z: [fields.len][:0]const u8 = undefined,
        allocated_z: usize = 0,

        fn deinit(self: *@This()) void {
            for (self.allocated_mem[0..self.allocated]) |slice| self.allocator.free(slice);
            for (self.allocated_mem_z[0..self.allocated_z]) |slice_z| self.allocator.free(slice_z);
        }

        fn clone(self: *@This(), comptime T: type, value: anytype) std.mem.Allocator.Error!T {
            return switch (@typeInfo(T)) {
                .pointer => |pointer| pointer: {
                    const cloned = if (pointer.sentinel_ptr == null) blk: {
                        const slice = try self.allocator.dupe(pointer.child, value);
                        self.allocated_mem[self.allocated] = slice;
                        self.allocated += 1;
                        break :blk slice;
                    } else blk: {
                        const slice_z = try self.allocator.dupeZ(pointer.child, value);
                        self.allocated_mem_z[self.allocated_z] = slice_z;
                        self.allocated_z += 1;
                        break :blk slice_z;
                    };

                    break :pointer switch (pointer.size) {
                        .slice => cloned,
                        .many => cloned.ptr,
                        else => @compileError("unsupported pointer size"),
                    };
                },

                .optional => |optional| if (value) |v| try self.clone(optional.child, v) else null,

                else => switch (T) {
                    zqlite.Blob => .{ .value = try self.clone(@FieldType(T, .value), value) },
                    else => value,
                },
            };
        }
    }{ .allocator = allocator };
    errdefer clone_ctx.deinit();

    inline for (fields) |field| {
        const value = column_fn(row, field);

        const target_field = &@field(target, @tagName(field));
        const TargetField = @TypeOf(target_field.*);

        target_field.* = try clone_ctx.clone(TargetField, value);
    }
}

pub fn freeStructFromRow(comptime Row: type, allocator: std.mem.Allocator, row: Row) void {
    const free = struct {
        fn free(alloc: std.mem.Allocator, value: anytype) void {
            const Value = @TypeOf(value);

            switch (@typeInfo(Value)) {
                .pointer => |pointer| alloc.free(switch (pointer.size) {
                    .slice => value,
                    .many => if (pointer.sentinel_ptr != null)
                        std.mem.span(value)
                    else
                        @compileError("many-item pointers only supported with sentinel"),
                    else => @compileError("unsupported pointer size"),
                }),

                .optional => if (value) |v| free(alloc, v),

                else => switch (Value) {
                    zqlite.Blob => alloc.free(value.value),
                    else => {},
                },
            }
        }
    }.free;

    inline for (@typeInfo(Row).@"struct".fields) |field| {
        const field_value = @field(row, field.name);

        free(allocator, field_value);
    }
}

test {
    std.testing.refAllDecls(@This());
}
