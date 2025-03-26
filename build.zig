const std = @import("std");
const utils = @import("utils").utils;

const Build = std.Build;

pub fn build(b: *Build) !void {
    const options = .{
        .target = b.standardTargetOptions(.{}),
        .optimize = b.standardOptimizeOption(.{}),
    };

    const zqlite_typed_mod = b.addModule("zqlite-typed", .{
        .root_source_file = b.path("src/root.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .imports = &.{
            .{ .name = "zqlite", .module = b.dependency("zqlite", options).module("zqlite") },
            .{ .name = "utils", .module = b.dependency("utils", options).module("utils") },
        },
    });

    const test_step = b.step("test", "Run unit tests");
    {
        const zqlite_typed_mod_test = b.addTest(.{
            .root_module = zqlite_typed_mod,
            .link_libc = true,
        });
        zqlite_typed_mod_test.linkSystemLibrary("sqlite3");

        const run_zqlite_typed_mod_test = b.addRunArtifact(zqlite_typed_mod_test);
        test_step.dependOn(&run_zqlite_typed_mod_test.step);
    }

    _ = utils.addCheckTls(b);
}
