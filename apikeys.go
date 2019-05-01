package main

// API Keys
const (
	APIKeyProduce                 int16 = 0
	APIKeyFetch                   int16 = 1
	APIKeyListOffsets             int16 = 2
	APIKeyMetadata                int16 = 3
	APIKeyLeaderAndIsr            int16 = 4
	APIKeyStopReplica             int16 = 5
	APIKeyUpdateMetadata          int16 = 6
	APIKeyControlledShutdown      int16 = 7
	APIKeyOffsetCommit            int16 = 8
	APIKeyOffsetFetch             int16 = 9
	APIKeyFindCoordinator         int16 = 10
	APIKeyJoinGroup               int16 = 11
	APIKeyHeartbeat               int16 = 12
	APIKeyLeaveGroup              int16 = 13
	APIKeySyncGroup               int16 = 14
	APIKeyDescribeGroups          int16 = 15
	APIKeyListGroups              int16 = 16
	APIKeySaslHandshake           int16 = 17
	APIKeyAPIVersions             int16 = 18
	APIKeyCreateTopics            int16 = 19
	APIKeyDeleteTopics            int16 = 20
	APIKeyDeleteRecords           int16 = 21
	APIKeyInitProducerID          int16 = 22
	APIKeyOffsetForLeaderEpoch    int16 = 23
	APIKeyAddPartitionsToTxn      int16 = 24
	APIKeyAddOffsetsToTxn         int16 = 25
	APIKeyEndTxn                  int16 = 26
	APIKeyWriteTxnMarkers         int16 = 27
	APIKeyTxnOffsetCommit         int16 = 28
	APIKeyDescribeAcls            int16 = 29
	APIKeyCreateAcls              int16 = 30
	APIKeyDeleteAcls              int16 = 31
	APIKeyDescribeConfigs         int16 = 32
	APIKeyAlterConfigs            int16 = 33
	APIKeyAlterReplicaLogDirs     int16 = 34
	APIKeyDescribeLogDirs         int16 = 35
	APIKeySaslAuthenticate        int16 = 36
	APIKeyCreatePartitions        int16 = 37
	APIKeyCreateDelegationToken   int16 = 38
	APIKeyRenewDelegationToken    int16 = 39
	APIKeyExpireDelegationToken   int16 = 40
	APIKeyDescribeDelegationToken int16 = 41
	APIKeyDeleteGroups            int16 = 42
	APIKeyElectPreferredLeaders   int16 = 43
)

var apiKeys = map[int16]string{
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
