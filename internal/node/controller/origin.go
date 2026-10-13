package controller

import (
	"fmt"
	"strings"
	"uuid"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

// originResolver is an immutable configuration snapshot. Each Resolve returns
// independently owned path memory for a single transaction.
type originResolver struct {
	centers map[uuid.UUID]string
	local   string
	servers map[uuid.UUID]bool
}

func newOriginResolver(local string, sets []*iproto.OriginUUIDSet) (*originResolver, error) {
	r := &originResolver{centers: map[uuid.UUID]string{}, servers: map[uuid.UUID]bool{}, local: local}
	if strings.TrimSpace(local) == "" {
		return nil, fmt.Errorf("controller: source center required")
	}
	for _, set := range sets {
		if strings.TrimSpace(set.GetCenterCode()) == "" {
			return nil, fmt.Errorf("controller: invalid origin center")
		}
		for _, text := range set.GetUuids() {
			id, err := uuid.Parse(text)
			if err != nil || id == uuid.Nil() {
				return nil, fmt.Errorf("controller: invalid origin UUID")
			}
			if owner, ok := r.centers[id]; ok && owner != set.GetCenterCode() {
				return nil, fmt.Errorf("controller: conflicting origin UUID ownership")
			}
			r.centers[id] = set.GetCenterCode()
		}
		for _, text := range set.GetServerUuids() {
			id, err := uuid.Parse(text)
			if err != nil || id == uuid.Nil() || r.centers[id] != set.GetCenterCode() {
				return nil, fmt.Errorf("controller: database origin is not registered to its center")
			}
			r.servers[id] = true
		}
	}
	return r, nil
}

func (r *originResolver) ResolveEndpoint(text string) (string, bool) {
	id, err := uuid.Parse(text)
	if err != nil || !r.servers[id] {
		return "", false
	}
	return id.String(), true
}
func (r *originResolver) Resolve(text string) (string, []string, uint32, bool) {
	id, err := uuid.Parse(text)
	if err != nil {
		return "", nil, 0, false
	}
	center, ok := r.centers[id]
	if !ok {
		return "", nil, 0, false
	}
	if center == r.local {
		return center, []string{center}, 1, true
	}
	// GTID proves the original and current center. Intermediate centers cannot
	// be recovered from a GTID and are not invented here.
	return center, []string{center, r.local}, 2, true
}

type sourceOriginFilter struct {
	plan  *rule.Plan
	local string
	mode  iproto.OriginPolicyMode
}

func (f sourceOriginFilter) AllowOrigin(center string, hops uint32, path []string) bool {
	if f.mode == iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_LOCAL_ONLY && center != f.local {
		return false
	}
	return f.plan.AllowOrigin(center, hops, path)
}
