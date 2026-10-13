package iproto

import (
	"fmt"
	"strings"

	"github.com/xmapst/xdts/internal/schemamig"
)

func ValidateDDLPolicy(policy *DDLPolicy) error {
	switch policy.GetMode() {
	case DDLMode_DDL_MODE_UNSPECIFIED, DDLMode_DDL_MODE_SYNC, DDLMode_DDL_MODE_IGNORE:
	case DDLMode_DDL_MODE_ONLY_FROM_CENTERS:
		if len(policy.GetOnlyFromCenters()) == 0 {
			return fmt.Errorf("iproto: DDL centers cannot be empty")
		}
		for _, center := range policy.GetOnlyFromCenters() {
			if strings.TrimSpace(center) == "" {
				return fmt.Errorf("iproto: invalid DDL center")
			}
		}
	default:
		return fmt.Errorf("iproto: unknown DDL mode")
	}
	if fallback := policy.GetDefinerFallback(); fallback != "" {
		if _, err := schemamig.ParseDefinerAccount(fallback); err != nil {
			return fmt.Errorf("iproto: invalid DDL definer fallback: %w", err)
		}
	}
	return nil
}
