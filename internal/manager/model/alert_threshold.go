package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
)

type AlertThreshold struct {
	Value         int64 `json:"value"`
	WindowSeconds int64 `json:"windowSeconds"`
}

func ParseAlertThreshold(raw JSON) (AlertThreshold, error) {
	v := AlertThreshold{WindowSeconds: 300}
	if len(raw) == 0 {
		return v, nil
	}
	if len(raw) > 1024 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return v, errors.New("model: invalid alert threshold")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return v, errors.New("model: invalid alert threshold")
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF || v.Value < 0 || v.Value > 1<<53-1 || v.WindowSeconds < 1 || v.WindowSeconds > 86400 {
		return v, errors.New("model: alert threshold outside supported range")
	}
	return v, nil
}

func ValidateAlertRuleConfiguration(r AlertRule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.Name) == "" || len([]rune(r.Name)) > 128 || r.SuppressSeconds < 0 || r.SuppressSeconds > 86400 {
		return errors.New("model: invalid alert rule configuration")
	}
	if _, err := ParseAlertThreshold(r.Threshold); err != nil {
		return err
	}
	if r.WebhookURL != "" {
		u, err := url.Parse(r.WebhookURL)
		if err != nil || len(r.WebhookURL) > 255 || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
			return errors.New("model: invalid alert webhook URL")
		}
	}
	return nil
}
