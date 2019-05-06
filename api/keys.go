package api

// Key represents the type for API keys
type Key int16

func (k Key) String() string {
	return keys[k]
}

// API Keys
const (
	KeyProduce                 Key = 0
	KeyFetch                   Key = 1
	KeyListOffsets             Key = 2
	KeyMetadata                Key = 3
	KeyLeaderAndIsr            Key = 4
	KeyStopReplica             Key = 5
	KeyUpdateMetadata          Key = 6
	KeyControlledShutdown      Key = 7
	KeyOffsetCommit            Key = 8
	KeyOffsetFetch             Key = 9
	KeyFindCoordinator         Key = 10
	KeyJoinGroup               Key = 11
	KeyHeartbeat               Key = 12
	KeyLeaveGroup              Key = 13
	KeySyncGroup               Key = 14
	KeyDescribeGroups          Key = 15
	KeyListGroups              Key = 16
	KeySaslHandshake           Key = 17
	KeyAPIVersions             Key = 18
	KeyCreateTopics            Key = 19
	KeyDeleteTopics            Key = 20
	KeyDeleteRecords           Key = 21
	KeyInitProducerID          Key = 22
	KeyOffsetForLeaderEpoch    Key = 23
	KeyAddPartitionsToTxn      Key = 24
	KeyAddOffsetsToTxn         Key = 25
	KeyEndTxn                  Key = 26
	KeyWriteTxnMarkers         Key = 27
	KeyTxnOffsetCommit         Key = 28
	KeyDescribeAcls            Key = 29
	KeyCreateAcls              Key = 30
	KeyDeleteAcls              Key = 31
	KeyDescribeConfigs         Key = 32
	KeyAlterConfigs            Key = 33
	KeyAlterReplicaLogDirs     Key = 34
	KeyDescribeLogDirs         Key = 35
	KeySaslAuthenticate        Key = 36
	KeyCreatePartitions        Key = 37
	KeyCreateDelegationToken   Key = 38
	KeyRenewDelegationToken    Key = 39
	KeyExpireDelegationToken   Key = 40
	KeyDescribeDelegationToken Key = 41
	KeyDeleteGroups            Key = 42
	KeyElectPreferredLeaders   Key = 43
)

var keys = map[Key]string{
	KeyProduce:                 "Produce",
	KeyFetch:                   "Fetch",
	KeyListOffsets:             "ListOffsets",
	KeyMetadata:                "Metadata",
	KeyLeaderAndIsr:            "LeaderAndIsr",
	KeyStopReplica:             "StopReplica",
	KeyUpdateMetadata:          "UpdateMetadata",
	KeyControlledShutdown:      "ControlledShutdown",
	KeyOffsetCommit:            "OffsetCommit",
	KeyOffsetFetch:             "OffsetFetch",
	KeyFindCoordinator:         "FindCoordinator",
	KeyJoinGroup:               "JoinGroup",
	KeyHeartbeat:               "Heartbeat",
	KeyLeaveGroup:              "LeaveGroup",
	KeySyncGroup:               "SyncGroup",
	KeyDescribeGroups:          "DescribeGroups",
	KeyListGroups:              "ListGroups",
	KeySaslHandshake:           "SaslHandshake",
	KeyAPIVersions:             "APIVersions",
	KeyCreateTopics:            "CreateTopics",
	KeyDeleteTopics:            "DeleteTopics",
	KeyDeleteRecords:           "DeleteRecords",
	KeyInitProducerID:          "InitProducerID",
	KeyOffsetForLeaderEpoch:    "OffsetForLeaderEpoch",
	KeyAddPartitionsToTxn:      "AddPartitionsToTxn",
	KeyAddOffsetsToTxn:         "AddOffsetsToTxn",
	KeyEndTxn:                  "EndTxn",
	KeyWriteTxnMarkers:         "WriteTxnMarkers",
	KeyTxnOffsetCommit:         "TxnOffsetCommit",
	KeyDescribeAcls:            "DescribeAcls",
	KeyCreateAcls:              "CreateAcls",
	KeyDeleteAcls:              "DeleteAcls",
	KeyDescribeConfigs:         "DescribeConfigs",
	KeyAlterConfigs:            "AlterConfigs",
	KeyAlterReplicaLogDirs:     "AlterReplicaLogDirs",
	KeyDescribeLogDirs:         "DescribeLogDirs",
	KeySaslAuthenticate:        "SaslAuthenticate",
	KeyCreatePartitions:        "CreatePartitions",
	KeyCreateDelegationToken:   "CreateDelegationToken",
	KeyRenewDelegationToken:    "RenewDelegationToken",
	KeyExpireDelegationToken:   "ExpireDelegationToken",
	KeyDescribeDelegationToken: "DescribeDelegationToken",
	KeyDeleteGroups:            "DeleteGroups",
	KeyElectPreferredLeaders:   "ElectPreferredLeaders",
}
