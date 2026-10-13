package iproto

import "errors"

const CapabilityPublicationWindow = "publication_window_v1"
const CapabilityReplicaSnapshot = "replica_snapshot_v1"
const CapabilityShadowTables = "shadow_tables_v1"
const CapabilityPersistentLineage = "database_lineage_store_v1"
const CapabilityNodeMigration = "node_migration_v1"
const CapabilitySnapshotPlanExtensions = "snapshot_plan_extensions_v1"
const CapabilitySnapshotMigration = "snapshot_migration_v1"
const CapabilityStoppedBacklogProbe = "stopped_backlog_probe_v1"
const CapabilitySnapshotResumeProof = "snapshot_resume_proof_v1"
const CapabilityFullOnlyStop = "full_only_stop_at_h_v1"
const CapabilitySnapshotTableCreation = "snapshot_table_creation_v1"
const CapabilityDDLSourceDependencies = "ddl_source_dependencies_v1"
const CapabilitySnapshotTableRecovery = "snapshot_table_recovery_v1"
const CapabilityDataCheck = "data_check_v1"
const CapabilityScriptETL = "script_etl_v1"

func CurrentNodeCapabilities() []string {
	return []string{CapabilityPublicationWindow, CapabilityReplicaSnapshot, CapabilityShadowTables, CapabilityPersistentLineage, CapabilityNodeMigration, CapabilitySnapshotPlanExtensions, CapabilitySnapshotMigration, CapabilityStoppedBacklogProbe, CapabilitySnapshotResumeProof, CapabilityFullOnlyStop, CapabilitySnapshotTableCreation, CapabilityDDLSourceDependencies, CapabilitySnapshotTableRecovery, CapabilityDataCheck, CapabilityScriptETL}
}

// Unknown bounded names remain usable by future builds. Feature users still
// require an exact explicitly advertised capability, never a version guess.
func ValidateNodeCapabilities(capabilities []string) error {
	if len(capabilities) > 16 {
		return errors.New("iproto: too many node capabilities")
	}
	seen := make(map[string]bool, len(capabilities))
	for _, capability := range capabilities {
		if len(capability) == 0 || len(capability) > 64 || seen[capability] {
			return errors.New("iproto: invalid node capability")
		}
		for _, c := range capability {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' && c != '.' {
				return errors.New("iproto: invalid node capability name")
			}
		}
		seen[capability] = true
	}
	return nil
}
