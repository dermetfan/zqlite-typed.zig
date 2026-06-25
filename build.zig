const std = @import("std");
const utils = @import("utils").utils;

const Build = std.Build;

pub fn build(b: *Build) !void {
    const options = .{
        .target = b.standardTargetOptions(.{}),
        .optimize = b.standardOptimizeOption(.{}),
    };

    const mod = b.addModule("zqlite-typed", .{
        .root_source_file = b.path("src/root.zig"),
        .target = options.target,
        .optimize = options.optimize,
        .imports = &.{
            .{ .name = "zqlite", .module = b.dependency("zqlite", options).module("zqlite") },
            .{ .name = "utils", .module = b.dependency("utils", options).module("utils") },
        },
        .link_libc = true,
    });
    mod.linkSystemLibrary("sqlite3", .{});

    const test_step = b.step("test", "Run unit tests");
    {
        const mod_test = b.addTest(.{
            .root_module = mod,
        });

        const run_mod_test = b.addRunArtifact(mod_test);
        test_step.dependOn(&run_mod_test.step);
    }

    _ = utils.addCheckTls(b);
}
