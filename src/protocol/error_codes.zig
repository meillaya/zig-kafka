pub const KafkaCode = enum(i16) {
    none = 0,
    unknown_server_error = -1,
    offset_out_of_range = 1,
    unknown_topic_or_partition = 3,
    leader_not_available = 5,
    not_leader_or_follower = 6,
    request_timed_out = 7,
    unsupported_version = 35,
    topic_authorization_failed = 29,
};

pub fn nameFor(code: i16) []const u8 {
    return switch (code) {
        0 => "NONE",
        -1 => "UNKNOWN_SERVER_ERROR",
        1 => "OFFSET_OUT_OF_RANGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        5 => "LEADER_NOT_AVAILABLE",
        6 => "NOT_LEADER_OR_FOLLOWER",
        7 => "REQUEST_TIMED_OUT",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        35 => "UNSUPPORTED_VERSION",
        else => "UNMAPPED_ERROR",
    };
}
