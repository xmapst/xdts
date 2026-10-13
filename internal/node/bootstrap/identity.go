// Package bootstrap resolves durable node identity before any business module
// starts. It uses the prepared store's startup ownership; it never opens xdoc.
package bootstrap

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	"uuid"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

const IdentityRecordID = "node"
const AttemptRecordID = "registration_attempt"

type Identity struct {
	NodeID           string    `bson:"node_id" json:"node_id"`
	Token            string    `bson:"token" json:"-"`
	Center           string    `bson:"center_code" json:"center_code"`
	ManagerPublicKey []byte    `bson:"manager_pub_key" json:"-"`
	RegisteredAt     time.Time `bson:"registered_at"`
}

func (i Identity) LogValue() slog.Value {
	return slog.GroupValue(slog.String("node_id", i.NodeID), slog.String("center_code", i.Center))
}
func (i Identity) String() string { return fmt.Sprintf("node=%s center=%s", i.NodeID, i.Center) }

type IStartupStore interface {
	Get(context.Context, string, string, any) error
	PutStartupCritical(context.Context, store.Op) error
}

type IRegistrar interface {
	Register(context.Context, *iproto.RegisterRequest) (*iproto.RegisterReply, error)
}

type Options struct {
	Store        IStartupStore
	Registrar    IRegistrar
	RegisterCode string
	Hostname     string
	IPs          []string
	OSArch       string
	Version      string
}

type Resolver struct{ opts Options }

func New(opts Options) *Resolver {
	opts.IPs = slices.Clone(opts.IPs)
	return &Resolver{opts: opts}
}

type registrationAttempt struct {
	InstanceID string `bson:"instance_id"`
}

func (r *Resolver) Resolve(ctx context.Context) (Identity, error) {
	if r.opts.Store == nil {
		return Identity{}, errors.New("bootstrap: identity store unavailable")
	}
	var saved Identity
	err := r.opts.Store.Get(ctx, "identity", IdentityRecordID, &saved)
	if err == nil {
		if err := validateIdentity(saved); err != nil {
			return Identity{}, err
		}
		if err := CheckRetirement(ctx, r.opts.Store, saved.NodeID); err != nil {
			return Identity{}, err
		}
		return saved, nil
	}
	if !errors.Is(err, xdoc.ErrNotFound) {
		return Identity{}, fmt.Errorf("bootstrap: read identity: %w", err)
	}
	if r.opts.Registrar == nil || strings.TrimSpace(r.opts.RegisterCode) == "" {
		return Identity{}, errors.New("bootstrap: registration requires a registrar and register_code")
	}
	var attempt registrationAttempt
	err = r.opts.Store.Get(ctx, "identity", AttemptRecordID, &attempt)
	if errors.Is(err, xdoc.ErrNotFound) {
		attempt.InstanceID = uuid.NewV7().String()
		if err := r.opts.Store.PutStartupCritical(ctx, store.Op{Collection: "identity", ID: AttemptRecordID, Doc: attempt, InsertOnly: true}); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
			return Identity{}, fmt.Errorf("bootstrap: persist registration instance: %w", err)
		}
		if err := r.opts.Store.Get(ctx, "identity", AttemptRecordID, &attempt); err != nil {
			return Identity{}, fmt.Errorf("bootstrap: read committed instance: %w", err)
		}
	} else if err != nil {
		return Identity{}, fmt.Errorf("bootstrap: read registration instance: %w", err)
	}
	if !canonicalUUID(attempt.InstanceID) {
		return Identity{}, errors.New("bootstrap: stored registration instance is invalid; restore identity or use a new register_code and data_dir")
	}
	request := &iproto.RegisterRequest{RegisterCode: r.opts.RegisterCode, InstanceId: attempt.InstanceID, Hostname: r.opts.Hostname, Ips: slices.Clone(r.opts.IPs), OsArch: r.opts.OSArch, NodeVersion: r.opts.Version, ProtocolVersion: iproto.CurrentProtocolVersion()}
	for {
		reply, err := r.opts.Registrar.Register(ctx, request)
		if err != nil {
			return Identity{}, fmt.Errorf("bootstrap: registration transport failed; restart to retry the saved instance: %w", err)
		}
		if reply == nil {
			return Identity{}, errors.New("bootstrap: empty registration reply")
		}
		if reply.ProtocolVersion != iproto.CurrentProtocolVersion() {
			return Identity{}, errors.New("bootstrap: manager protocol version mismatch")
		}
		if reply.Error == nil {
			return r.saveIdentity(ctx, Identity{NodeID: reply.NodeId, Token: reply.NodeToken, Center: reply.CenterCode, ManagerPublicKey: slices.Clone(reply.ManagerPubKey), RegisteredAt: time.Now().UTC()})
		}
		if reply.Error.Code != "SERVICE_UNAVAILABLE" && reply.Error.Code != "INTERNAL" {
			return Identity{}, &RegistrationError{Code: reply.Error.Code}
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Identity{}, fmt.Errorf("bootstrap: registration service remained unavailable: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

type RegistrationError struct{ Code string }

func (e *RegistrationError) Error() string {
	code := e.Code
	switch code {
	case "REGISTER_CODE_NOT_FOUND", "REGISTER_CODE_REVOKED", "REGISTER_CODE_EXPIRED", "REGISTER_CODE_EXHAUSTED", "INVALID_INSTANCE_ID", "REGISTRATION_INSTANCE_CONFLICT", "INVALID_REGISTRATION_METADATA", "NODE_IDENTITY_RETIRED", "REGISTER_CENTER_NOT_FOUND", "PROTOCOL_INCOMPATIBLE", "SERVICE_UNAVAILABLE", "INTERNAL":
	default:
		code = "UNKNOWN"
	}
	return "bootstrap: registration rejected (" + code + "); check the register_code and center, or use a new code and data_dir for a retired identity"
}

func (r *Resolver) saveIdentity(ctx context.Context, identity Identity) (Identity, error) {
	if err := validateIdentity(identity); err != nil {
		return Identity{}, err
	}
	if err := r.opts.Store.PutStartupCritical(ctx, store.Op{Collection: "identity", ID: IdentityRecordID, Doc: identity, InsertOnly: true}); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return Identity{}, fmt.Errorf("bootstrap: persist identity: %w", err)
	}
	var saved Identity
	if err := r.opts.Store.Get(ctx, "identity", IdentityRecordID, &saved); err != nil {
		return Identity{}, fmt.Errorf("bootstrap: read committed identity: %w", err)
	}
	if err := validateIdentity(saved); err != nil {
		return Identity{}, err
	}
	if saved.NodeID != identity.NodeID || saved.Token != identity.Token || saved.Center != identity.Center || !slices.Equal(saved.ManagerPublicKey, identity.ManagerPublicKey) {
		return Identity{}, errors.New("bootstrap: committed identity conflicts with registration reply")
	}
	return saved, nil
}

func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.String() != "00000000-0000-0000-0000-000000000000"
}

func validateIdentity(identity Identity) error {
	if !canonicalUUID(identity.NodeID) || strings.TrimSpace(identity.Token) == "" || len(identity.Token) > 256 || !utf8.ValidString(identity.Center) || utf8.RuneCountInString(identity.Center) == 0 || utf8.RuneCountInString(identity.Center) > 64 {
		return errors.New("bootstrap: invalid node identity; restore the saved identity or register with a new data_dir")
	}
	for _, c := range identity.Center {
		if c == '*' || c == '#' || unicode.IsControl(c) || unicode.IsSpace(c) {
			return errors.New("bootstrap: invalid center routing code")
		}
	}
	if len(identity.ManagerPublicKey) != 0 && len(identity.ManagerPublicKey) != ed25519.PublicKeySize {
		return errors.New("bootstrap: invalid manager public key")
	}
	return nil
}
