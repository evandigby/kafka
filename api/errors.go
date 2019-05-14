package api

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Kafka Errors
const (
	ErrorUnknownServerError                 int16 = -1 // The server experienced an unexpected error when processing the request.
	ErrorNone                               int16 = 0  // No Error (Success)
	ErrorOffsetOutOfRange                   int16 = 1  // The requested offset is not within the range of offsets maintained by the server.
	ErrorCorruptMessage                     int16 = 2  // This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
	ErrorUnknownTopicOrPartition            int16 = 3  // This server does not host this topic-partition.
	ErrorInvalidFetchSize                   int16 = 4  // The requested fetch size is invalid.
	ErrorLeaderNotAvailable                 int16 = 5  // There is no leader for this topic-partition as we are in the middle of a leadership election.
	ErrorNotLeaderForPartition              int16 = 6  // This server is not the leader for that topic-partition.
	ErrorRequestTimedOut                    int16 = 7  // The request timed out.
	ErrorBrokerNotAvailable                 int16 = 8  // The broker is not available.
	ErrorReplicaNotAvailable                int16 = 9  // The replica is not available for the requested topic-partition.
	ErrorMessageTooLarge                    int16 = 10 // The request included a message larger than the max message size the server will accept.
	ErrorStaleControllerEpoch               int16 = 11 // The controller moved to another broker.
	ErrorOffsetMetadataTooLarge             int16 = 12 // The metadata field of the offset request was too large.
	ErrorNetworkException                   int16 = 13 // The server disconnected before a response was received.
	ErrorCoordinatorLoadInProgress          int16 = 14 // The coordinator is loading and hence can't process requests.
	ErrorCoordinatorNotAvailable            int16 = 15 // The coordinator is not available.
	ErrorNotCoordinator                     int16 = 16 // This is not the correct coordinator.
	ErrorInvalidTopicException              int16 = 17 // The request attempted to perform an operation on an invalid topic.
	ErrorRecordListTooLarge                 int16 = 18 // The request included message batch larger than the configured segment size on the server.
	ErrorNotEnoughReplicas                  int16 = 19 // Messages are rejected since there are fewer in-sync replicas than required.
	ErrorNotEnoughReplicasAfterAppend       int16 = 20 // Messages are written to the log, but to fewer in-sync replicas than required.
	ErrorInvalidRequiredAcks                int16 = 21 // Produce request specified an invalid value for required acks.
	ErrorIllegalGeneration                  int16 = 22 // Specified group generation id is not valid.
	ErrorInconsistentGroupProtocol          int16 = 23 // The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
	ErrorInvalidGroupID                     int16 = 24 // The configured groupId is invalid.
	ErrorUnknownMemberID                    int16 = 25 // The coordinator is not aware of this member.
	ErrorInvalidSessionTimeout              int16 = 26 // The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
	ErrorRebalanceInProgress                int16 = 27 // The group is rebalancing, so a rejoin is needed.
	ErrorInvalidCommitOffsetSize            int16 = 28 // The committing offset data size is not valid.
	ErrorTopicAuthorizationFailed           int16 = 29 // Not authorized to access topics: [Topic authorization failed.]
	ErrorGroupAuthorizationFailed           int16 = 30 // Not authorized to access group: Group authorization failed.
	ErrorClusterAuthorizationFailed         int16 = 31 // Cluster authorization failed.
	ErrorInvalidTimestamp                   int16 = 32 // The timestamp of the message is out of acceptable range.
	ErrorUnsupportedSaslMechanism           int16 = 33 // The broker does not support the requested SASL mechanism.
	ErrorIllegalSaslState                   int16 = 34 // Request is not valid given the current SASL state.
	ErrorUnsupportedVersion                 int16 = 35 // The version of API is not supported.
	ErrorTopicAlreadyExists                 int16 = 36 // Topic with this name already exists.
	ErrorInvalidPartitions                  int16 = 37 // Number of partitions is below 1.
	ErrorInvalidReplicationFactor           int16 = 38 // Replication factor is below 1 or larger than the number of available brokers.
	ErrorInvalidReplicaAssignment           int16 = 39 // Replica assignment is invalid.
	ErrorInvalidConfig                      int16 = 40 // Configuration is invalid.
	ErrorNotController                      int16 = 41 // This is not the correct controller for this cluster.
	ErrorInvalidRequest                     int16 = 42 // This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
	ErrorUnsupportedForMessageFormat        int16 = 43 // The message format version on the broker does not support the request.
	ErrorPolicyViolation                    int16 = 44 // Request parameters do not satisfy the configured policy.
	ErrorOutOfOrderSequenceNumber           int16 = 45 // The broker received an out of order sequence number.
	ErrorDuplicateSequenceNumber            int16 = 46 // The broker received a duplicate sequence number.
	ErrorInvalidProducerEpoch               int16 = 47 // Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
	ErrorInvalidTxnState                    int16 = 48 // The producer attempted a transactional operation in an invalid state.
	ErrorInvalidProducerIDMapping           int16 = 49 // The producer attempted to use a producer id which is not currently assigned to its transactional id.
	ErrorInvalidTransactionTimeout          int16 = 50 // The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
	ErrorConcurrentTransactions             int16 = 51 // The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
	ErrorTransactionCoordinatorFenced       int16 = 52 // Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
	ErrorTransactionalIDAuthorizationFailed int16 = 53 // Transactional Id authorization failed.
	ErrorSecurityDisabled                   int16 = 54 // Security features are disabled.
	ErrorOperationNotAttempted              int16 = 55 // The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
	ErrorKafkaStorageError                  int16 = 56 // Disk error when trying to access log file on the disk.
	ErrorLogDirNotFound                     int16 = 57 // The user-specified log directory is not found in the broker config.
	ErrorSaslAuthenticationFailed           int16 = 58 // SASL Authentication failed.
	ErrorUnknownProducerID                  int16 = 59 // This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.
	ErrorReassignmentInProgress             int16 = 60 // A partition reassignment is in progress.
	ErrorDelegationTokenAuthDisabled        int16 = 61 // Delegation Token feature is not enabled.
	ErrorDelegationTokenNotFound            int16 = 62 // Delegation Token is not found on server.
	ErrorDelegationTokenOwnerMismatch       int16 = 63 // Specified Principal is not valid Owner/Renewer.
	ErrorDelegationTokenRequestNotAllowed   int16 = 64 // Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
	ErrorDelegationTokenAuthorizationFailed int16 = 65 // Delegation Token authorization failed.
	ErrorDelegationTokenExpired             int16 = 66 // Delegation Token is expired.
	ErrorInvalidPrincipalType               int16 = 67 // Supplied principalType is not supported.
	ErrorNonEmptyGroup                      int16 = 68 // The group is not empty.
	ErrorGroupIDNotFound                    int16 = 69 // The group id does not exist.
	ErrorFetchSessionIDNotFound             int16 = 70 // The fetch session ID was not found.
	ErrorInvalidFetchSessionEpoch           int16 = 71 // The fetch session epoch is invalid.
	ErrorListenerNotFound                   int16 = 72 // There is no listener on the leader broker that matches the listener on which metadata request was processed.
	ErrorTopicDeletionDisabled              int16 = 73 // Topic deletion is disabled.
	ErrorFencedLeaderEpoch                  int16 = 74 // The leader epoch in the request is older than the epoch on the broker
	ErrorUnknownLeaderEpoch                 int16 = 75 // The leader epoch in the request is newer than the epoch on the broker
	ErrorUnsupportedCompressionType         int16 = 76 // The requesting client does not support the compression type of given partition.
	ErrorStaleBrokerEpoch                   int16 = 77 // Broker epoch has changed
	ErrorOffsetNotAvailable                 int16 = 78 // The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing
	ErrorMemberIDRequired                   int16 = 79 // The group member needs to have a valid member id before actually entering a consumer group
	ErrorPreferredLeaderNotAvailable        int16 = 80 // The preferred leader was not available
	ErrorGroupMaxSizeReached                int16 = 81 // Consumer group The consumer group has reached its max size. already has the configured maximum number of members.
)

var kafkaErrors = map[int16]string{
	ErrorUnknownServerError:                 "UNKNOWN_SERVER_ERROR",
	ErrorNone:                               "NONE",
	ErrorOffsetOutOfRange:                   "OFFSET_OUT_OF_RANGE",
	ErrorCorruptMessage:                     "CORRUPT_MESSAGE",
	ErrorUnknownTopicOrPartition:            "UNKNOWN_TOPIC_OR_PARTITION",
	ErrorInvalidFetchSize:                   "INVALID_FETCH_SIZE",
	ErrorLeaderNotAvailable:                 "LEADER_NOT_AVAILABLE",
	ErrorNotLeaderForPartition:              "NOT_LEADER_FOR_PARTITION",
	ErrorRequestTimedOut:                    "REQUEST_TIMED_OUT",
	ErrorBrokerNotAvailable:                 "BROKER_NOT_AVAILABLE",
	ErrorReplicaNotAvailable:                "REPLICA_NOT_AVAILABLE",
	ErrorMessageTooLarge:                    "MESSAGE_TOO_LARGE",
	ErrorStaleControllerEpoch:               "STALE_CONTROLLER_EPOCH",
	ErrorOffsetMetadataTooLarge:             "OFFSET_METADATA_TOO_LARGE",
	ErrorNetworkException:                   "NETWORK_EXCEPTION",
	ErrorCoordinatorLoadInProgress:          "COORDINATOR_LOAD_IN_PROGRESS",
	ErrorCoordinatorNotAvailable:            "COORDINATOR_NOT_AVAILABLE",
	ErrorNotCoordinator:                     "NOT_COORDINATOR",
	ErrorInvalidTopicException:              "INVALID_TOPIC_EXCEPTION",
	ErrorRecordListTooLarge:                 "RECORD_LIST_TOO_LARGE",
	ErrorNotEnoughReplicas:                  "NOT_ENOUGH_REPLICAS",
	ErrorNotEnoughReplicasAfterAppend:       "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
	ErrorInvalidRequiredAcks:                "INVALID_REQUIRED_ACKS",
	ErrorIllegalGeneration:                  "ILLEGAL_GENERATION",
	ErrorInconsistentGroupProtocol:          "INCONSISTENT_GROUP_PROTOCOL",
	ErrorInvalidGroupID:                     "INVALID_GROUP_ID",
	ErrorUnknownMemberID:                    "UNKNOWN_MEMBER_ID",
	ErrorInvalidSessionTimeout:              "INVALID_SESSION_TIMEOUT",
	ErrorRebalanceInProgress:                "REBALANCE_IN_PROGRESS",
	ErrorInvalidCommitOffsetSize:            "INVALID_COMMIT_OFFSET_SIZE",
	ErrorTopicAuthorizationFailed:           "TOPIC_AUTHORIZATION_FAILED",
	ErrorGroupAuthorizationFailed:           "GROUP_AUTHORIZATION_FAILED",
	ErrorClusterAuthorizationFailed:         "CLUSTER_AUTHORIZATION_FAILED",
	ErrorInvalidTimestamp:                   "INVALID_TIMESTAMP",
	ErrorUnsupportedSaslMechanism:           "UNSUPPORTED_SASL_MECHANISM",
	ErrorIllegalSaslState:                   "ILLEGAL_SASL_STATE",
	ErrorUnsupportedVersion:                 "UNSUPPORTED_VERSION",
	ErrorTopicAlreadyExists:                 "TOPIC_ALREADY_EXISTS",
	ErrorInvalidPartitions:                  "INVALID_PARTITIONS",
	ErrorInvalidReplicationFactor:           "INVALID_REPLICATION_FACTOR",
	ErrorInvalidReplicaAssignment:           "INVALID_REPLICA_ASSIGNMENT",
	ErrorInvalidConfig:                      "INVALID_CONFIG",
	ErrorNotController:                      "NOT_CONTROLLER",
	ErrorInvalidRequest:                     "INVALID_REQUEST",
	ErrorUnsupportedForMessageFormat:        "UNSUPPORTED_FOR_MESSAGE_FORMAT",
	ErrorPolicyViolation:                    "POLICY_VIOLATION",
	ErrorOutOfOrderSequenceNumber:           "OUT_OF_ORDER_SEQUENCE_NUMBER",
	ErrorDuplicateSequenceNumber:            "DUPLICATE_SEQUENCE_NUMBER",
	ErrorInvalidProducerEpoch:               "INVALID_PRODUCER_EPOCH",
	ErrorInvalidTxnState:                    "INVALID_TXN_STATE",
	ErrorInvalidProducerIDMapping:           "INVALID_PRODUCER_ID_MAPPING",
	ErrorInvalidTransactionTimeout:          "INVALID_TRANSACTION_TIMEOUT",
	ErrorConcurrentTransactions:             "CONCURRENT_TRANSACTIONS",
	ErrorTransactionCoordinatorFenced:       "TRANSACTION_COORDINATOR_FENCED",
	ErrorTransactionalIDAuthorizationFailed: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
	ErrorSecurityDisabled:                   "SECURITY_DISABLED",
	ErrorOperationNotAttempted:              "OPERATION_NOT_ATTEMPTED",
	ErrorKafkaStorageError:                  "KAFKA_STORAGE_ERROR",
	ErrorLogDirNotFound:                     "LOG_DIR_NOT_FOUND",
	ErrorSaslAuthenticationFailed:           "SASL_AUTHENTICATION_FAILED",
	ErrorUnknownProducerID:                  "UNKNOWN_PRODUCER_ID",
	ErrorReassignmentInProgress:             "REASSIGNMENT_IN_PROGRESS",
	ErrorDelegationTokenAuthDisabled:        "DELEGATION_TOKEN_AUTH_DISABLED",
	ErrorDelegationTokenNotFound:            "DELEGATION_TOKEN_NOT_FOUND",
	ErrorDelegationTokenOwnerMismatch:       "DELEGATION_TOKEN_OWNER_MISMATCH",
	ErrorDelegationTokenRequestNotAllowed:   "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
	ErrorDelegationTokenAuthorizationFailed: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
	ErrorDelegationTokenExpired:             "DELEGATION_TOKEN_EXPIRED",
	ErrorInvalidPrincipalType:               "INVALID_PRINCIPAL_TYPE",
	ErrorNonEmptyGroup:                      "NON_EMPTY_GROUP",
	ErrorGroupIDNotFound:                    "GROUP_ID_NOT_FOUND",
	ErrorFetchSessionIDNotFound:             "FETCH_SESSION_ID_NOT_FOUND",
	ErrorInvalidFetchSessionEpoch:           "INVALID_FETCH_SESSION_EPOCH",
	ErrorListenerNotFound:                   "LISTENER_NOT_FOUND",
	ErrorTopicDeletionDisabled:              "TOPIC_DELETION_DISABLED",
	ErrorFencedLeaderEpoch:                  "FENCED_LEADER_EPOCH",
	ErrorUnknownLeaderEpoch:                 "UNKNOWN_LEADER_EPOCH",
	ErrorUnsupportedCompressionType:         "UNSUPPORTED_COMPRESSION_TYPE",
	ErrorStaleBrokerEpoch:                   "STALE_BROKER_EPOCH",
	ErrorOffsetNotAvailable:                 "OFFSET_NOT_AVAILABLE",
	ErrorMemberIDRequired:                   "MEMBER_ID_REQUIRED",
	ErrorPreferredLeaderNotAvailable:        "PREFERRED_LEADER_NOT_AVAILABLE",
	ErrorGroupMaxSizeReached:                "GROUP_MAX_SIZE_REACHED",
}

var errorsFriendly = map[int16]string{
	ErrorUnknownServerError:                 "The server experienced an unexpected error when processing the request.",
	ErrorNone:                               "No Error (Success)",
	ErrorOffsetOutOfRange:                   "The requested offset is not within the range of offsets maintained by the server.",
	ErrorCorruptMessage:                     "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
	ErrorUnknownTopicOrPartition:            "This server does not host this topic-partition.",
	ErrorInvalidFetchSize:                   "The requested fetch size is invalid.",
	ErrorLeaderNotAvailable:                 "There is no leader for this topic-partition as we are in the middle of a leadership election.",
	ErrorNotLeaderForPartition:              "This server is not the leader for that topic-partition.",
	ErrorRequestTimedOut:                    "The request timed out.",
	ErrorBrokerNotAvailable:                 "The broker is not available.",
	ErrorReplicaNotAvailable:                "The replica is not available for the requested topic-partition.",
	ErrorMessageTooLarge:                    "The request included a message larger than the max message size the server will accept.",
	ErrorStaleControllerEpoch:               "The controller moved to another broker.",
	ErrorOffsetMetadataTooLarge:             "The metadata field of the offset request was too large.",
	ErrorNetworkException:                   "The server disconnected before a response was received.",
	ErrorCoordinatorLoadInProgress:          "The coordinator is loading and hence can't process requests.",
	ErrorCoordinatorNotAvailable:            "The coordinator is not available.",
	ErrorNotCoordinator:                     "This is not the correct coordinator.",
	ErrorInvalidTopicException:              "The request attempted to perform an operation on an invalid topic.",
	ErrorRecordListTooLarge:                 "The request included message batch larger than the configured segment size on the server.",
	ErrorNotEnoughReplicas:                  "Messages are rejected since there are fewer in-sync replicas than required.",
	ErrorNotEnoughReplicasAfterAppend:       "Messages are written to the log, but to fewer in-sync replicas than required.",
	ErrorInvalidRequiredAcks:                "Produce request specified an invalid value for required acks.",
	ErrorIllegalGeneration:                  "Specified group generation id is not valid.",
	ErrorInconsistentGroupProtocol:          "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
	ErrorInvalidGroupID:                     "The configured groupId is invalid.",
	ErrorUnknownMemberID:                    "The coordinator is not aware of this member.",
	ErrorInvalidSessionTimeout:              "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
	ErrorRebalanceInProgress:                "The group is rebalancing, so a rejoin is needed.",
	ErrorInvalidCommitOffsetSize:            "The committing offset data size is not valid.",
	ErrorTopicAuthorizationFailed:           "Not authorized to access topics: [Topic authorization failed.]",
	ErrorGroupAuthorizationFailed:           "Not authorized to access group: Group authorization failed.",
	ErrorClusterAuthorizationFailed:         "Cluster authorization failed.",
	ErrorInvalidTimestamp:                   "The timestamp of the message is out of acceptable range.",
	ErrorUnsupportedSaslMechanism:           "The broker does not support the requested SASL mechanism.",
	ErrorIllegalSaslState:                   "Request is not valid given the current SASL state.",
	ErrorUnsupportedVersion:                 "The version of API is not supported.",
	ErrorTopicAlreadyExists:                 "Topic with this name already exists.",
	ErrorInvalidPartitions:                  "Number of partitions is below 1.",
	ErrorInvalidReplicationFactor:           "Replication factor is below 1 or larger than the number of available brokers.",
	ErrorInvalidReplicaAssignment:           "Replica assignment is invalid.",
	ErrorInvalidConfig:                      "Configuration is invalid.",
	ErrorNotController:                      "This is not the correct controller for this cluster.",
	ErrorInvalidRequest:                     "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
	ErrorUnsupportedForMessageFormat:        "The message format version on the broker does not support the request.",
	ErrorPolicyViolation:                    "Request parameters do not satisfy the configured policy.",
	ErrorOutOfOrderSequenceNumber:           "The broker received an out of order sequence number.",
	ErrorDuplicateSequenceNumber:            "The broker received a duplicate sequence number.",
	ErrorInvalidProducerEpoch:               "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.",
	ErrorInvalidTxnState:                    "The producer attempted a transactional operation in an invalid state.",
	ErrorInvalidProducerIDMapping:           "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
	ErrorInvalidTransactionTimeout:          "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
	ErrorConcurrentTransactions:             "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
	ErrorTransactionCoordinatorFenced:       "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
	ErrorTransactionalIDAuthorizationFailed: "Transactional Id authorization failed.",
	ErrorSecurityDisabled:                   "Security features are disabled.",
	ErrorOperationNotAttempted:              "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
	ErrorKafkaStorageError:                  "Disk error when trying to access log file on the disk.",
	ErrorLogDirNotFound:                     "The user-specified log directory is not found in the broker config.",
	ErrorSaslAuthenticationFailed:           "SASL Authentication failed.",
	ErrorUnknownProducerID:                  "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
	ErrorReassignmentInProgress:             "A partition reassignment is in progress.",
	ErrorDelegationTokenAuthDisabled:        "Delegation Token feature is not enabled.",
	ErrorDelegationTokenNotFound:            "Delegation Token is not found on server.",
	ErrorDelegationTokenOwnerMismatch:       "Specified Principal is not valid Owner/Renewer.",
	ErrorDelegationTokenRequestNotAllowed:   "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
	ErrorDelegationTokenAuthorizationFailed: "Delegation Token authorization failed.",
	ErrorDelegationTokenExpired:             "Delegation Token is expired.",
	ErrorInvalidPrincipalType:               "Supplied principalType is not supported.",
	ErrorNonEmptyGroup:                      "The group is not empty.",
	ErrorGroupIDNotFound:                    "The group id does not exist.",
	ErrorFetchSessionIDNotFound:             "The fetch session ID was not found.",
	ErrorInvalidFetchSessionEpoch:           "The fetch session epoch is invalid.",
	ErrorListenerNotFound:                   "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
	ErrorTopicDeletionDisabled:              "Topic deletion is disabled.",
	ErrorFencedLeaderEpoch:                  "The leader epoch in the request is older than the epoch on the broker",
	ErrorUnknownLeaderEpoch:                 "The leader epoch in the request is newer than the epoch on the broker",
	ErrorUnsupportedCompressionType:         "The requesting client does not support the compression type of given partition.",
	ErrorStaleBrokerEpoch:                   "Broker epoch has changed",
	ErrorOffsetNotAvailable:                 "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing",
	ErrorMemberIDRequired:                   "The group member needs to have a valid member id before actually entering a consumer group",
	ErrorPreferredLeaderNotAvailable:        "The preferred leader was not available",
	ErrorGroupMaxSizeReached:                "Consumer group The consumer group has reached its max size. already has the configured maximum number of members.",
}

var retriable = map[int16]bool{
	ErrorUnknownServerError:                 false,
	ErrorNone:                               false,
	ErrorOffsetOutOfRange:                   false,
	ErrorCorruptMessage:                     true,
	ErrorUnknownTopicOrPartition:            true,
	ErrorInvalidFetchSize:                   false,
	ErrorLeaderNotAvailable:                 true,
	ErrorNotLeaderForPartition:              true,
	ErrorRequestTimedOut:                    true,
	ErrorBrokerNotAvailable:                 false,
	ErrorReplicaNotAvailable:                false,
	ErrorMessageTooLarge:                    false,
	ErrorStaleControllerEpoch:               false,
	ErrorOffsetMetadataTooLarge:             false,
	ErrorNetworkException:                   true,
	ErrorCoordinatorLoadInProgress:          true,
	ErrorCoordinatorNotAvailable:            true,
	ErrorNotCoordinator:                     true,
	ErrorInvalidTopicException:              false,
	ErrorRecordListTooLarge:                 false,
	ErrorNotEnoughReplicas:                  true,
	ErrorNotEnoughReplicasAfterAppend:       true,
	ErrorInvalidRequiredAcks:                false,
	ErrorIllegalGeneration:                  false,
	ErrorInconsistentGroupProtocol:          false,
	ErrorInvalidGroupID:                     false,
	ErrorUnknownMemberID:                    false,
	ErrorInvalidSessionTimeout:              false,
	ErrorRebalanceInProgress:                false,
	ErrorInvalidCommitOffsetSize:            false,
	ErrorTopicAuthorizationFailed:           false,
	ErrorGroupAuthorizationFailed:           false,
	ErrorClusterAuthorizationFailed:         false,
	ErrorInvalidTimestamp:                   false,
	ErrorUnsupportedSaslMechanism:           false,
	ErrorIllegalSaslState:                   false,
	ErrorUnsupportedVersion:                 false,
	ErrorTopicAlreadyExists:                 false,
	ErrorInvalidPartitions:                  false,
	ErrorInvalidReplicationFactor:           false,
	ErrorInvalidReplicaAssignment:           false,
	ErrorInvalidConfig:                      false,
	ErrorNotController:                      true,
	ErrorInvalidRequest:                     false,
	ErrorUnsupportedForMessageFormat:        false,
	ErrorPolicyViolation:                    false,
	ErrorOutOfOrderSequenceNumber:           false,
	ErrorDuplicateSequenceNumber:            false,
	ErrorInvalidProducerEpoch:               false,
	ErrorInvalidTxnState:                    false,
	ErrorInvalidProducerIDMapping:           false,
	ErrorInvalidTransactionTimeout:          false,
	ErrorConcurrentTransactions:             false,
	ErrorTransactionCoordinatorFenced:       false,
	ErrorTransactionalIDAuthorizationFailed: false,
	ErrorSecurityDisabled:                   false,
	ErrorOperationNotAttempted:              false,
	ErrorKafkaStorageError:                  true,
	ErrorLogDirNotFound:                     false,
	ErrorSaslAuthenticationFailed:           false,
	ErrorUnknownProducerID:                  false,
	ErrorReassignmentInProgress:             false,
	ErrorDelegationTokenAuthDisabled:        false,
	ErrorDelegationTokenNotFound:            false,
	ErrorDelegationTokenOwnerMismatch:       false,
	ErrorDelegationTokenRequestNotAllowed:   false,
	ErrorDelegationTokenAuthorizationFailed: false,
	ErrorDelegationTokenExpired:             false,
	ErrorInvalidPrincipalType:               false,
	ErrorNonEmptyGroup:                      false,
	ErrorGroupIDNotFound:                    false,
	ErrorFetchSessionIDNotFound:             true,
	ErrorInvalidFetchSessionEpoch:           true,
	ErrorListenerNotFound:                   true,
	ErrorTopicDeletionDisabled:              false,
	ErrorFencedLeaderEpoch:                  true,
	ErrorUnknownLeaderEpoch:                 true,
	ErrorUnsupportedCompressionType:         false,
	ErrorStaleBrokerEpoch:                   false,
	ErrorOffsetNotAvailable:                 true,
	ErrorMemberIDRequired:                   false,
	ErrorPreferredLeaderNotAvailable:        true,
	ErrorGroupMaxSizeReached:                false,
}

// ErrorFromReader creates an error from a reader by reading a 16 bit (2 bytes) value from the reader. It returns nil when "no error" is received to keep with Go semantics.
func ErrorFromReader(r io.Reader) error {
	var kerr Error
	err := binary.Read(r, binary.BigEndian, &kerr.Code)
	if err != nil {
		return err
	}

	if kerr.Code != ErrorNone {
		return &kerr
	}
	return nil
}

// Error represents a kafka error
type Error struct {
	Code int16
}

func (e *Error) Error() string {
	return fmt.Sprintf("Kafka Error %v (%v): %v", kafkaErrors[e.Code], e.Code, errorsFriendly[e.Code])
}

// IsKafkaError returns whether or not the current error is a Kafka error, as opposed to an error communicating with Kafka.
func IsKafkaError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

// IsKafkaCode compares code to the current kafka error code. If it's not a kafka error it will return false
func IsKafkaCode(err error, code int16) bool {
	ke, ok := err.(*Error)
	if !ok {
		return false
	}

	return ke.Code == code
}
