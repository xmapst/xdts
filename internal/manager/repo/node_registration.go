package repo

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRegistrationInstanceInvalid  = errors.New("repo: invalid registration instance UUID")
	ErrRegistrationInstanceConflict = errors.New("repo: registration instance belongs to another code")
	ErrRegistrationMetadataInvalid  = errors.New("repo: invalid registration metadata")
	ErrNodeIdentityRetired          = errors.New("repo: registered node identity was retired")
)

const registrationTokenVersion uint32 = 1

// NodeRegistrationResult contains the original reply. Tokens are never stored
// in the management database or included in printable representations.
type NodeRegistrationResult struct {
	NodeID     uuidx.UUID
	Token      string `json:"-"`
	CenterCode string
}

func (r NodeRegistrationResult) String() string {
	return fmt.Sprintf("node=%s center=%s", r.NodeID, r.CenterCode)
}

type nodeRegistrationAttempt struct {
	RegisterCodeID uuidx.UUID `gorm:"column:register_code_id;primaryKey"`
	InstanceID     uuidx.UUID `gorm:"column:instance_id"`
	NodeID         uuidx.UUID `gorm:"column:node_id"`
	CenterCode     string     `gorm:"column:center_code"`
	TokenVersion   uint32     `gorm:"column:token_version"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
}

func (nodeRegistrationAttempt) TableName() string { return "node_registration_attempts" }

type NodeRegistrations struct{ db *gorm.DB }

func NewNodeRegistrations(db *gorm.DB) *NodeRegistrations { return &NodeRegistrations{db: db} }

// Register commits consumption, identity and replay metadata together. The
// original high entropy code is required to reproduce a token after a lost
// reply; neither that code nor the token is saved in the management database.
func (r *NodeRegistrations) Register(ctx context.Context, instanceID string, params RegisterNodeParams, now time.Time) (NodeRegistrationResult, error) {
	instance, err := uuidx.Parse(instanceID)
	if err != nil || instance.IsZero() || instance.String() != instanceID {
		return NodeRegistrationResult{}, ErrRegistrationInstanceInvalid
	}
	if err := validateRegistrationMetadata(params); err != nil {
		return NodeRegistrationResult{}, err
	}
	if r.db == nil {
		return NodeRegistrationResult{}, errors.New("repo: registration database unavailable")
	}
	var result NodeRegistrationResult
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var code model.NodeRegisterCode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&code, "code_hash=?", hashRegisterCode(params.PlainRegisterCode)).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRegisterCodeNotFound
			}
			return err
		}
		var attempt nodeRegistrationAttempt
		err := tx.First(&attempt, "instance_id=?", instance).Error
		if err == nil {
			if attempt.RegisterCodeID != code.ID {
				return ErrRegistrationInstanceConflict
			}
			var node model.Node
			if err := tx.Unscoped().Clauses(clause.Locking{Strength: "SHARE"}).First(&node, "id=?", attempt.NodeID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrNodeIdentityRetired
				}
				return err
			}
			if node.DeletedAt.Valid || node.Status == model.NodeStatusRevoked || node.RegisterCodeID != code.ID || attempt.TokenVersion != registrationTokenVersion {
				return ErrNodeIdentityRetired
			}
			token := registrationToken(params.PlainRegisterCode, instance, node.ID)
			if subtle.ConstantTimeCompare([]byte(hashNodeToken(token)), []byte(node.TokenHash)) != 1 {
				return ErrNodeIdentityRetired
			}
			result = NodeRegistrationResult{NodeID: node.ID, Token: token, CenterCode: attempt.CenterCode}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := checkNodeRegisterCodeUsable(&code, now); err != nil {
			return err
		}
		var center model.Center
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(&center, "id=?", code.CenterID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrCenterNotFound
			}
			return err
		}
		name := params.Name
		if name == "" {
			name = code.PresetName
		}
		if utf8.RuneCountInString(name) > 128 || !utf8.ValidString(name) {
			return ErrRegistrationMetadataInvalid
		}
		id := uuidx.New()
		if strings.TrimSpace(name) == "" {
			name = "node-" + id.String()
		}
		if err := normalizeResourceName(&name); err != nil {
			return err
		}
		token := registrationToken(params.PlainRegisterCode, instance, id)
		node := &model.Node{ID: id, Name: name, CenterID: code.CenterID, Labels: code.PresetLabels,
			Hostname: params.Hostname, IPs: params.IPs, OSArch: params.OSArch, Version: params.Version,
			TokenHash: hashNodeToken(token), Status: model.NodeStatusRegistering, RegisteredAt: now,
			RegisterCodeID: code.ID, CleanupState: model.CleanupStateDone, LogLevel: model.LogLevelInfo, LogShipLevel: model.LogLevelInfo}
		if err := node.Validate(); err != nil {
			return err
		}
		if _, err := ConsumeNodeRegisterCode(ctx, tx, params.PlainRegisterCode, now); err != nil {
			return err
		}
		if err := tx.Create(node).Error; err != nil {
			if duplicateIndex(err, "uk_nodes_name") {
				return ErrNodeNameConflict
			}
			return err
		}
		attempt = nodeRegistrationAttempt{RegisterCodeID: code.ID, InstanceID: instance, NodeID: id, CenterCode: center.Code, TokenVersion: registrationTokenVersion, CreatedAt: now}
		if err := tx.Create(&attempt).Error; err != nil {
			if isDuplicateKeyError(err) {
				return ErrRegistrationInstanceConflict
			}
			return err
		}
		result = NodeRegistrationResult{NodeID: id, Token: token, CenterCode: center.Code}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return NodeRegistrationResult{}, err
	}
	return result, nil
}

func registrationToken(code string, instance, node uuidx.UUID) string {
	mac := hmac.New(sha256.New, []byte(code))
	mac.Write([]byte("xdts/node-registration/token/v1\x00"))
	mac.Write([]byte(instance.String()))
	mac.Write([]byte{0})
	mac.Write([]byte(node.String()))
	return hex.EncodeToString(mac.Sum(nil))
}

func validateRegistrationMetadata(p RegisterNodeParams) error {
	for _, field := range []struct {
		value string
		limit int
	}{{p.Name, 128}, {p.Hostname, 255}, {p.IPs, 255}, {p.OSArch, 64}, {p.Version, 32}} {
		if !utf8.ValidString(field.value) || utf8.RuneCountInString(field.value) > field.limit || strings.ContainsRune(field.value, '\x00') {
			return ErrRegistrationMetadataInvalid
		}
	}
	if len(p.PlainRegisterCode) > 256 {
		return ErrRegistrationMetadataInvalid
	}
	return nil
}
