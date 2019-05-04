package kafka

// APIKey represents the type for API keys
type APIKey int16

// API Keys
const (
	APIKeyProduce                 APIKey = 0
	APIKeyFetch                   APIKey = 1
	APIKeyListOffsets             APIKey = 2
	APIKeyMetadata                APIKey = 3
	APIKeyLeaderAndIsr            APIKey = 4
	APIKeyStopReplica             APIKey = 5
	APIKeyUpdateMetadata          APIKey = 6
	APIKeyControlledShutdown      APIKey = 7
	APIKeyOffsetCommit            APIKey = 8
	APIKeyOffsetFetch             APIKey = 9
	APIKeyFindCoordinator         APIKey = 10
	APIKeyJoinGroup               APIKey = 11
	APIKeyHeartbeat               APIKey = 12
	APIKeyLeaveGroup              APIKey = 13
	APIKeySyncGroup               APIKey = 14
	APIKeyDescribeGroups          APIKey = 15
	APIKeyListGroups              APIKey = 16
	APIKeySaslHandshake           APIKey = 17
	APIKeyAPIVersions             APIKey = 18
	APIKeyCreateTopics            APIKey = 19
	APIKeyDeleteTopics            APIKey = 20
	APIKeyDeleteRecords           APIKey = 21
	APIKeyInitProducerID          APIKey = 22
	APIKeyOffsetForLeaderEpoch    APIKey = 23
	APIKeyAddPartitionsToTxn      APIKey = 24
	APIKeyAddOffsetsToTxn         APIKey = 25
	APIKeyEndTxn                  APIKey = 26
	APIKeyWriteTxnMarkers         APIKey = 27
	APIKeyTxnOffsetCommit         APIKey = 28
	APIKeyDescribeAcls            APIKey = 29
	APIKeyCreateAcls              APIKey = 30
	APIKeyDeleteAcls              APIKey = 31
	APIKeyDescribeConfigs         APIKey = 32
	APIKeyAlterConfigs            APIKey = 33
	APIKeyAlterReplicaLogDirs     APIKey = 34
	APIKeyDescribeLogDirs         APIKey = 35
	APIKeySaslAuthenticate        APIKey = 36
	APIKeyCreatePartitions        APIKey = 37
	APIKeyCreateDelegationToken   APIKey = 38
	APIKeyRenewDelegationToken    APIKey = 39
	APIKeyExpireDelegationToken   APIKey = 40
	APIKeyDescribeDelegationToken APIKey = 41
	APIKeyDeleteGroups            APIKey = 42
	APIKeyElectPreferredLeaders   APIKey = 43
)

var apiKeys = map[APIKey]string{
	APIKeyProduce:                 "Produce",
	APIKeyFetch:                   "Fetch",
	APIKeyListOffsets:             "ListOffsets",
	APIKeyMetadata:                "Metadata",
	APIKeyLeaderAndIsr:            "LeaderAndIsr",
	APIKeyStopReplica:             "StopReplica",
	APIKeyUpdateMetadata:          "UpdateMetadata",
	APIKeyControlledShutdown:      "ControlledShutdown",
	APIKeyOffsetCommit:            "OffsetCommit",
	APIKeyOffsetFetch:             "OffsetFetch",
	APIKeyFindCoordinator:         "FindCoordinator",
	APIKeyJoinGroup:               "JoinGroup",
	APIKeyHeartbeat:               "Heartbeat",
	APIKeyLeaveGroup:              "LeaveGroup",
	APIKeySyncGroup:               "SyncGroup",
	APIKeyDescribeGroups:          "DescribeGroups",
	APIKeyListGroups:              "ListGroups",
	APIKeySaslHandshake:           "SaslHandshake",
	APIKeyAPIVersions:             "APIVersions",
	APIKeyCreateTopics:            "CreateTopics",
	APIKeyDeleteTopics:            "DeleteTopics",
	APIKeyDeleteRecords:           "DeleteRecords",
	APIKeyInitProducerID:          "InitProducerID",
	APIKeyOffsetForLeaderEpoch:    "OffsetForLeaderEpoch",
	APIKeyAddPartitionsToTxn:      "AddPartitionsToTxn",
	APIKeyAddOffsetsToTxn:         "AddOffsetsToTxn",
	APIKeyEndTxn:                  "EndTxn",
	APIKeyWriteTxnMarkers:         "WriteTxnMarkers",
	APIKeyTxnOffsetCommit:         "TxnOffsetCommit",
	APIKeyDescribeAcls:            "DescribeAcls",
	APIKeyCreateAcls:              "CreateAcls",
	APIKeyDeleteAcls:              "DeleteAcls",
	APIKeyDescribeConfigs:         "DescribeConfigs",
	APIKeyAlterConfigs:            "AlterConfigs",
	APIKeyAlterReplicaLogDirs:     "AlterReplicaLogDirs",
	APIKeyDescribeLogDirs:         "DescribeLogDirs",
	APIKeySaslAuthenticate:        "SaslAuthenticate",
	APIKeyCreatePartitions:        "CreatePartitions",
	APIKeyCreateDelegationToken:   "CreateDelegationToken",
	APIKeyRenewDelegationToken:    "RenewDelegationToken",
	APIKeyExpireDelegationToken:   "ExpireDelegationToken",
	APIKeyDescribeDelegationToken: "DescribeDelegationToken",
	APIKeyDeleteGroups:            "DeleteGroups",
	APIKeyElectPreferredLeaders:   "ElectPreferredLeaders",
}
