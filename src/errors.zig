pub const KafkaError = error{
    InvalidBrokerAddress,
    InvalidData,
    UnexpectedEOF,
    NegativeLength,
    UnsupportedVersion,
    ApiNotSupported,
    CorrelationIdMismatch,
    UnknownTopicOrPartition,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
    TopicAuthorizationFailed,
    OffsetOutOfRange,
    NoBrokersAvailable,
    NoBrokerForPartition,
    MissingEnvironment,
    InvalidCrc,
    BrokerError,
};

pub fn fromErrorCode(code: i16) ?KafkaError {
    return switch (code) {
        0 => null,
        1 => error.OffsetOutOfRange,
        3 => error.UnknownTopicOrPartition,
        5 => error.LeaderNotAvailable,
        6 => error.NotLeaderOrFollower,
        7 => error.RequestTimedOut,
        29 => error.TopicAuthorizationFailed,
        35 => error.UnsupportedVersion,
        else => error.BrokerError,
    };
}
