const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const lib_mod = b.addModule("zig-kafka", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/unit/all_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_tests.root_module.addImport("zig-kafka", lib_mod);

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration/all_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    integration_tests.root_module.addImport("zig-kafka", lib_mod);

    const run_integration_tests = b.addRunArtifact(integration_tests);
    if (b.args) |args| run_integration_tests.addArgs(args);
    const integration_step = b.step("test-integration", "Run integration tests against Kafka");
    integration_step.dependOn(&run_integration_tests.step);

    const broker_mod = b.createModule(.{
        .root_source_file = b.path("examples/broker.zig"),
        .target = target,
        .optimize = optimize,
    });
    broker_mod.addImport("zig-kafka", lib_mod);

    const broker_exe = b.addExecutable(.{
        .name = "broker",
        .root_module = broker_mod,
    });
    b.installArtifact(broker_exe);

    const run_broker = b.addRunArtifact(broker_exe);
    if (b.args) |args| run_broker.addArgs(args);
    const run_broker_step = b.step("run-broker", "Run the educational Kafka broker");
    run_broker_step.dependOn(&run_broker.step);

    const example_mod = b.createModule(.{
        .root_source_file = b.path("examples/roundtrip.zig"),
        .target = target,
        .optimize = optimize,
    });
    example_mod.addImport("zig-kafka", lib_mod);

    const example_exe = b.addExecutable(.{
        .name = "roundtrip",
        .root_module = example_mod,
    });
    b.installArtifact(example_exe);

    const run_example = b.addRunArtifact(example_exe);
    if (b.args) |args| run_example.addArgs(args);
    const run_example_step = b.step("run-roundtrip", "Run the Kafka roundtrip example");
    run_example_step.dependOn(&run_example.step);
}
