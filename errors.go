package main

import "fmt"

// Kafka Errors
const (
	KafkaErrorUnknownServerError                 int16 = -1 //	False	The server experienced an unexpected error when processing the request.
	KafkaErrorNone                               int16 = 0  //	False
	KafkaErrorOffsetOutOfRange                   int16 = 1  //	False	The requested offset is not within the range of offsets maintained by the server.
	KafkaErrorCorruptMessage                     int16 = 2  //	True	This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
	KafkaErrorUnknownTopicOrPartition            int16 = 3  //	True	This server does not host this topic-partition.
	KafkaErrorInvalidFetchSize                   int16 = 4  //	False	The requested fetch size is invalid.
	KafkaErrorLeaderNotAvailable                 int16 = 5  //	True	There is no leader for this topic-partition as we are in the middle of a leadership election.
	KafkaErrorNotLeaderForPartition              int16 = 6  //	True	This server is not the leader for that topic-partition.
	KafkaErrorRequestTimedOut                    int16 = 7  //	True	The request timed out.
	KafkaErrorBrokerNotAvailable                 int16 = 8  //	False	The broker is not available.
	KafkaErrorReplicaNotAvailable                int16 = 9  //	False	The replica is not available for the requested topic-partition.
	KafkaErrorMessageTooLarge                    int16 = 10 //	False	The request included a message larger than the max message size the server will accept.
	KafkaErrorStaleControllerEpoch               int16 = 11 //	False	The controller moved to another broker.
	KafkaErrorOffsetMetadataTooLarge             int16 = 12 //	False	The metadata field of the offset request was too large.
	KafkaErrorNetworkException                   int16 = 13 //	True	The server disconnected before a response was received.
	KafkaErrorCoordinatorLoadInProgress          int16 = 14 //	True	The coordinator is loading and hence can't process requests.
	KafkaErrorCoordinatorNotAvailable            int16 = 15 //	True	The coordinator is not available.
	KafkaErrorNotCoordinator                     int16 = 16 //	True	This is not the correct coordinator.
	KafkaErrorInvalidTopicException              int16 = 17 //	False	The request attempted to perform an operation on an invalid topic.
	KafkaErrorRecordListTooLarge                 int16 = 18 //	False	The request included message batch larger than the configured segment size on the server.
	KafkaErrorNotEnoughReplicas                  int16 = 19 //	True	Messages are rejected since there are fewer in-sync replicas than required.
	KafkaErrorNotEnoughReplicasAfterAppend       int16 = 20 //	True	Messages are written to the log, but to fewer in-sync replicas than required.
	KafkaErrorInvalidRequiredAcks                int16 = 21 //	False	Produce request specified an invalid value for required acks.
	KafkaErrorIllegalGeneration                  int16 = 22 //	False	Specified group generation id is not valid.
	KafkaErrorInconsistentGroupProtocol          int16 = 23 //	False	The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
	KafkaErrorInvalidGroupID                     int16 = 24 //	False	The configured groupId is invalid.
	KafkaErrorUnknownMemberID                    int16 = 25 //	False	The coordinator is not aware of this member.
	KafkaErrorInvalidSessionTimeout              int16 = 26 //	False	The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
	KafkaErrorRebalanceInProgress                int16 = 27 //	False	The group is rebalancing, so a rejoin is needed.
	KafkaErrorInvalidCommitOffsetSize            int16 = 28 //	False	The committing offset data size is not valid.
	KafkaErrorTopicAuthorizationFailed           int16 = 29 //	False	Not authorized to access topics: [Topic authorization failed.]
	KafkaErrorGroupAuthorizationFailed           int16 = 30 //	False	Not authorized to access group: Group authorization failed.
	KafkaErrorClusterAuthorizationFailed         int16 = 31 //	False	Cluster authorization failed.
	KafkaErrorInvalidTimestamp                   int16 = 32 //	False	The timestamp of the message is out of acceptable range.
	KafkaErrorUnsupportedSaslMechanism           int16 = 33 //	False	The broker does not support the requested SASL mechanism.
	KafkaErrorIllegalSaslState                   int16 = 34 //	False	Request is not valid given the current SASL state.
	KafkaErrorUnsupportedVersion                 int16 = 35 //	False	The version of API is not supported.
	KafkaErrorTopicAlreadyExists                 int16 = 36 //	False	Topic with this name already exists.
	KafkaErrorInvalidPartitions                  int16 = 37 //	False	Number of partitions is below 1.
	KafkaErrorInvalidReplicationFactor           int16 = 38 //	False	Replication factor is below 1 or larger than the number of available brokers.
	KafkaErrorInvalidReplicaAssignment           int16 = 39 //	False	Replica assignment is invalid.
	KafkaErrorInvalidConfig                      int16 = 40 //	False	Configuration is invalid.
	KafkaErrorNotController                      int16 = 41 //	True	This is not the correct controller for this cluster.
	KafkaErrorInvalidRequest                     int16 = 42 //	False	This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
	KafkaErrorUnsupportedForMessageFormat        int16 = 43 //	False	The message format version on the broker does not support the request.
	KafkaErrorPolicyViolation                    int16 = 44 //	False	Request parameters do not satisfy the configured policy.
	KafkaErrorOutOfOrderSequenceNumber           int16 = 45 //	False	The broker received an out of order sequence number.
	KafkaErrorDuplicateSequenceNumber            int16 = 46 //	False	The broker received a duplicate sequence number.
	KafkaErrorInvalidProducerEpoch               int16 = 47 //	False	Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
	KafkaErrorInvalidTxnState                    int16 = 48 //	False	The producer attempted a transactional operation in an invalid state.
	KafkaErrorInvalidProducerIDMapping           int16 = 49 //	False	The producer attempted to use a producer id which is not currently assigned to its transactional id.
	KafkaErrorInvalidTransactionTimeout          int16 = 50 //	False	The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
	KafkaErrorConcurrentTransactions             int16 = 51 //	False	The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
	KafkaErrorTransactionCoordinatorFenced       int16 = 52 //	False	Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
	KafkaErrorTransactionalIDAuthorizationFailed int16 = 53 //	False	Transactional Id authorization failed.
	KafkaErrorSecurityDisabled                   int16 = 54 //	False	Security features are disabled.
	KafkaErrorOperationNotAttempted              int16 = 55 //	False	The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
	KafkaErrorKafkaStorageError                  int16 = 56 //	True	Disk error when trying to access log file on the disk.
	KafkaErrorLogDirNotFound                     int16 = 57 //	False	The user-specified log directory is not found in the broker config.
	KafkaErrorSaslAuthenticationFailed           int16 = 58 //	False	SASL Authentication failed.
	KafkaErrorUnknownProducerID                  int16 = 59 //	False	This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.
	KafkaErrorReassignmentInProgress             int16 = 60 //	False	A partition reassignment is in progress.
	KafkaErrorDelegationTokenAuthDisabled        int16 = 61 //	False	Delegation Token feature is not enabled.
	KafkaErrorDelegationTokenNotFound            int16 = 62 //	False	Delegation Token is not found on server.
	KafkaErrorDelegationTokenOwnerMismatch       int16 = 63 //	False	Specified Principal is not valid Owner/Renewer.
	KafkaErrorDelegationTokenRequestNotAllowed   int16 = 64 //	False	Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
	KafkaErrorDelegationTokenAuthorizationFailed int16 = 65 //	False	Delegation Token authorization failed.
	KafkaErrorDelegationTokenExpired             int16 = 66 //	False	Delegation Token is expired.
	KafkaErrorInvalidPrincipalType               int16 = 67 //	False	Supplied principalType is not supported.
	KafkaErrorNonEmptyGroup                      int16 = 68 //	False	The group is not empty.
	KafkaErrorGroupIDNotFound                    int16 = 69 //	False	The group id does not exist.
	KafkaErrorFetchSessionIDNotFound             int16 = 70 //	True	The fetch session ID was not found.
	KafkaErrorInvalidFetchSessionEpoch           int16 = 71 //	True	The fetch session epoch is invalid.
	KafkaErrorListenerNotFound                   int16 = 72 //	True	There is no listener on the leader broker that matches the listener on which metadata request was processed.
	KafkaErrorTopicDeletionDisabled              int16 = 73 //	False	Topic deletion is disabled.
	KafkaErrorFencedLeaderEpoch                  int16 = 74 //	True	The leader epoch in the request is older than the epoch on the broker
	KafkaErrorUnknownLeaderEpoch                 int16 = 75 //	True	The leader epoch in the request is newer than the epoch on the broker
	KafkaErrorUnsupportedCompressionType         int16 = 76 //	False	The requesting client does not support the compression type of given partition.
	KafkaErrorStaleBrokerEpoch                   int16 = 77 //	False	Broker epoch has changed
	KafkaErrorOffsetNotAvailable                 int16 = 78 //	True	The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing
	KafkaErrorMemberIDRequired                   int16 = 79 //	False	The group member needs to have a valid member id before actually entering a consumer group
	KafkaErrorPreferredLeaderNotAvailable        int16 = 80 //	True	The preferred leader was not available
	KafkaErrorGroupMaxSizeReached                int16 = 81 //	False	Consumer group The consumer group has reached its max size. already has the configured maximum number of members.
)

var kafkaErrors = map[int16]string{
	KafkaErrorUnknownServerError:                 "UNKNOWN_SERVER_ERROR",
	KafkaErrorNone:                               "NONE",
	KafkaErrorOffsetOutOfRange:                   "OFFSET_OUT_OF_RANGE",
	KafkaErrorCorruptMessage:                     "CORRUPT_MESSAGE",
	KafkaErrorUnknownTopicOrPartition:            "UNKNOWN_TOPIC_OR_PARTITION",
	KafkaErrorInvalidFetchSize:                   "INVALID_FETCH_SIZE",
	KafkaErrorLeaderNotAvailable:                 "LEADER_NOT_AVAILABLE",
	KafkaErrorNotLeaderForPartition:              "NOT_LEADER_FOR_PARTITION",
	KafkaErrorRequestTimedOut:                    "REQUEST_TIMED_OUT",
	KafkaErrorBrokerNotAvailable:                 "BROKER_NOT_AVAILABLE",
	KafkaErrorReplicaNotAvailable:                "REPLICA_NOT_AVAILABLE",
	KafkaErrorMessageTooLarge:                    "MESSAGE_TOO_LARGE",
	KafkaErrorStaleControllerEpoch:               "STALE_CONTROLLER_EPOCH",
	KafkaErrorOffsetMetadataTooLarge:             "OFFSET_METADATA_TOO_LARGE",
	KafkaErrorNetworkException:                   "NETWORK_EXCEPTION",
	KafkaErrorCoordinatorLoadInProgress:          "COORDINATOR_LOAD_IN_PROGRESS",
	KafkaErrorCoordinatorNotAvailable:            "COORDINATOR_NOT_AVAILABLE",
	KafkaErrorNotCoordinator:                     "NOT_COORDINATOR",
	KafkaErrorInvalidTopicException:              "INVALID_TOPIC_EXCEPTION",
	KafkaErrorRecordListTooLarge:                 "RECORD_LIST_TOO_LARGE",
	KafkaErrorNotEnoughReplicas:                  "NOT_ENOUGH_REPLICAS",
	KafkaErrorNotEnoughReplicasAfterAppend:       "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
	KafkaErrorInvalidRequiredAcks:                "INVALID_REQUIRED_ACKS",
	KafkaErrorIllegalGeneration:                  "ILLEGAL_GENERATION",
	KafkaErrorInconsistentGroupProtocol:          "INCONSISTENT_GROUP_PROTOCOL",
	KafkaErrorInvalidGroupID:                     "INVALID_GROUP_ID",
	KafkaErrorUnknownMemberID:                    "UNKNOWN_MEMBER_ID",
	KafkaErrorInvalidSessionTimeout:              "INVALID_SESSION_TIMEOUT",
	KafkaErrorRebalanceInProgress:                "REBALANCE_IN_PROGRESS",
	KafkaErrorInvalidCommitOffsetSize:            "INVALID_COMMIT_OFFSET_SIZE",
	KafkaErrorTopicAuthorizationFailed:           "TOPIC_AUTHORIZATION_FAILED",
	KafkaErrorGroupAuthorizationFailed:           "GROUP_AUTHORIZATION_FAILED",
	KafkaErrorClusterAuthorizationFailed:         "CLUSTER_AUTHORIZATION_FAILED",
	KafkaErrorInvalidTimestamp:                   "INVALID_TIMESTAMP",
	KafkaErrorUnsupportedSaslMechanism:           "UNSUPPORTED_SASL_MECHANISM",
	KafkaErrorIllegalSaslState:                   "ILLEGAL_SASL_STATE",
	KafkaErrorUnsupportedVersion:                 "UNSUPPORTED_VERSION",
	KafkaErrorTopicAlreadyExists:                 "TOPIC_ALREADY_EXISTS",
	KafkaErrorInvalidPartitions:                  "INVALID_PARTITIONS",
	KafkaErrorInvalidReplicationFactor:           "INVALID_REPLICATION_FACTOR",
	KafkaErrorInvalidReplicaAssignment:           "INVALID_REPLICA_ASSIGNMENT",
	KafkaErrorInvalidConfig:                      "INVALID_CONFIG",
	KafkaErrorNotController:                      "NOT_CONTROLLER",
	KafkaErrorInvalidRequest:                     "INVALID_REQUEST",
	KafkaErrorUnsupportedForMessageFormat:        "UNSUPPORTED_FOR_MESSAGE_FORMAT",
	KafkaErrorPolicyViolation:                    "POLICY_VIOLATION",
	KafkaErrorOutOfOrderSequenceNumber:           "OUT_OF_ORDER_SEQUENCE_NUMBER",
	KafkaErrorDuplicateSequenceNumber:            "DUPLICATE_SEQUENCE_NUMBER",
	KafkaErrorInvalidProducerEpoch:               "INVALID_PRODUCER_EPOCH",
	KafkaErrorInvalidTxnState:                    "INVALID_TXN_STATE",
	KafkaErrorInvalidProducerIDMapping:           "INVALID_PRODUCER_ID_MAPPING",
	KafkaErrorInvalidTransactionTimeout:          "INVALID_TRANSACTION_TIMEOUT",
	KafkaErrorConcurrentTransactions:             "CONCURRENT_TRANSACTIONS",
	KafkaErrorTransactionCoordinatorFenced:       "TRANSACTION_COORDINATOR_FENCED",
	KafkaErrorTransactionalIDAuthorizationFailed: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
	KafkaErrorSecurityDisabled:                   "SECURITY_DISABLED",
	KafkaErrorOperationNotAttempted:              "OPERATION_NOT_ATTEMPTED",
	KafkaErrorKafkaStorageError:                  "KAFKA_STORAGE_ERROR",
	KafkaErrorLogDirNotFound:                     "LOG_DIR_NOT_FOUND",
	KafkaErrorSaslAuthenticationFailed:           "SASL_AUTHENTICATION_FAILED",
	KafkaErrorUnknownProducerID:                  "UNKNOWN_PRODUCER_ID",
	KafkaErrorReassignmentInProgress:             "REASSIGNMENT_IN_PROGRESS",
	KafkaErrorDelegationTokenAuthDisabled:        "DELEGATION_TOKEN_AUTH_DISABLED",
	KafkaErrorDelegationTokenNotFound:            "DELEGATION_TOKEN_NOT_FOUND",
	KafkaErrorDelegationTokenOwnerMismatch:       "DELEGATION_TOKEN_OWNER_MISMATCH",
	KafkaErrorDelegationTokenRequestNotAllowed:   "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
	KafkaErrorDelegationTokenAuthorizationFailed: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
	KafkaErrorDelegationTokenExpired:             "DELEGATION_TOKEN_EXPIRED",
	KafkaErrorInvalidPrincipalType:               "INVALID_PRINCIPAL_TYPE",
	KafkaErrorNonEmptyGroup:                      "NON_EMPTY_GROUP",
	KafkaErrorGroupIDNotFound:                    "GROUP_ID_NOT_FOUND",
	KafkaErrorFetchSessionIDNotFound:             "FETCH_SESSION_ID_NOT_FOUND",
	KafkaErrorInvalidFetchSessionEpoch:           "INVALID_FETCH_SESSION_EPOCH",
	KafkaErrorListenerNotFound:                   "LISTENER_NOT_FOUND",
	KafkaErrorTopicDeletionDisabled:              "TOPIC_DELETION_DISABLED",
	KafkaErrorFencedLeaderEpoch:                  "FENCED_LEADER_EPOCH",
	KafkaErrorUnknownLeaderEpoch:                 "UNKNOWN_LEADER_EPOCH",
	KafkaErrorUnsupportedCompressionType:         "UNSUPPORTED_COMPRESSION_TYPE",
	KafkaErrorStaleBrokerEpoch:                   "STALE_BROKER_EPOCH",
	KafkaErrorOffsetNotAvailable:                 "OFFSET_NOT_AVAILABLE",
	KafkaErrorMemberIDRequired:                   "MEMBER_ID_REQUIRED",
	KafkaErrorPreferredLeaderNotAvailable:        "PREFERRED_LEADER_NOT_AVAILABLE",
	KafkaErrorGroupMaxSizeReached:                "GROUP_MAX_SIZE_REACHED",
}

type kafkaError struct {
	code int16
}

func (e *kafkaError) Error() string {
	return fmt.Sprintf("Kafka Error %v: %v", e.code, kafkaErrors[e.code])
}
