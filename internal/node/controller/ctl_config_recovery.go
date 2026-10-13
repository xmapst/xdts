package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

const timerConfigRecovery = "controller.config_recovery"

// Recovery signals are consumed in the controller loop. Pending work survives
// startup and failed pulls; repeated recoveries merge into one follow-up pull.
func (m *Module) onConfigRecoveryTimer(_ int64, _ map[string]string) {
	select {
	case _, open := <-m.recoveryEvents:
		if open {
			m.configRepullPending = true
		} else {
			m.recoveryEvents = nil
		}
	default:
	}
	if m.identityRetired || !m.mirrorReady || !m.configRepullPending || m.configRepullBusy || m.now().Before(m.configRepullAfter) {
		return
	}
	m.configRepullPending, m.configRepullBusy = false, true
	var reply *iproto.ConfigPullReply
	var err error
	m.AsyncGo("controller.config_recovery", func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTargetedPullTimeout)
		defer cancel()
		reply, err = m.pullConfig(ctx, nil)
	}, func() { m.finishConfigRecoveryPull(reply, err) })
}

func (m *Module) finishConfigRecoveryPull(reply *iproto.ConfigPullReply, err error) {
	m.configRepullBusy = false
	if m.identityRetired {
		m.configRepullPending = false
		return
	}
	if err != nil {
		m.configRepullPending = true
		delay := time.Second
		for n := 0; n < m.configRepullAttempt && delay < 30*time.Second; n++ {
			delay *= 2
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		if m.configRepullAttempt < 6 {
			m.configRepullAttempt++
		}
		m.configRepullAfter = m.now().Add(delay)
		slog.Warn("full config pull after connection recovery failed", slog.Any("error", err), slog.Duration("retry_after", delay))
		return
	}
	m.configRepullAttempt, m.configRepullAfter = 0, time.Time{}
	slog.Info("full config pull after connection recovery succeeded", slog.Int("pipeline_count", len(reply.GetConfigs())))
	m.applyPlans(diffConfigs(m.applied, reply.GetConfigs(), true))
}
