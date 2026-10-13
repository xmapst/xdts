package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/xmapst/xdoc"
)

// AmbiguousCredentialError 表示 INVALID_PASSWORD 无法区分口令错误和 identity 文件损坏。
// 必须等待人工核对，不移动或重建文件，以免错误口令导致有效状态丢失。
type AmbiguousCredentialError struct {
	Path string
}

func (e *AmbiguousCredentialError) Error() string {
	return fmt.Sprintf(
		"store: 打开 %s 失败，口令错误与文件损坏无法区分（xdoc 对两者返回相同的 "+
			"INVALID_PASSWORD 错误）：可能是 register_code 配置错误，也可能是该文件已损坏。"+
			"节点停在等待人工处置状态，未移动或重建任何文件；请核对 register_code 后重启，"+
			"或在确认文件确实损坏后手动处置（NODE-XDOC-11）", e.Path)
}

const invalidPasswordMarker = "INVALID_PASSWORD"

const notEncryptedMarker = "NOT_ENCRYPTED"

func isInvalidPasswordError(err error) bool {
	return err != nil && strings.Contains(err.Error(), invalidPasswordMarker)
}

// selfCheck 按 NODE-XDOC-11 的三分支流程打开 identity.xdoc 与 state.xdoc：
//
//  1. 先开 identity.xdoc。
//     - 成功（或文件本不存在、Open 就地新建）→ 口令已被证明正确。
//     - 报 ErrNeedsRebuild → 明确损坏（与口令无关），直接走 NODE-XDOC-06 重建。
//     - 报 INVALID_PASSWORD → 真正歧义，返回 AmbiguousCredentialError，
//     不触碰任何文件。
//  2. identity.xdoc 就绪后开 state.xdoc：此时口令已证明正确，ErrNeedsRebuild
//     与 INVALID_PASSWORD 都只可能是损坏，统一走 NODE-XDOC-06 重建。
//
// 失败时保证不留下已打开但未归还的句柄。
func (m *Module) selfCheck(ctx context.Context) (identityDB, stateDB *xdoc.DB, err error) {
	identityPath := filepath.Join(m.opts.DataDir, identityFileName)
	statePath := filepath.Join(m.opts.DataDir, stateFileName)

	identityDB, err = xdoc.Open(identityPath, xdoc.WithPassword(m.opts.RegisterCode))
	switch {
	case err == nil:
		// 口令已被证明正确（含 identity.xdoc 原本不存在、Open 就地新建的
		// 首次启动场景——NODE-XDOC-11 第 2 条：不属于歧义场景）。
	case errors.Is(err, xdoc.ErrNeedsRebuild):
		identityDB, err = m.rebuildFile(ctx, identityPath, identityFileName)
		if err != nil {
			return nil, nil, err
		}
	case isInvalidPasswordError(err):
		return nil, nil, &AmbiguousCredentialError{Path: identityPath}
	default:
		return nil, nil, fmt.Errorf("store: 打开 identity.xdoc 失败: %w", err)
	}

	// Diagnose existing state without allowing the disk engine to initialize or
	// truncate an invalid header. Identity has already verified the password.
	if _, statErr := os.Stat(statePath); statErr == nil {
		ro, readErr := xdoc.Open(statePath, xdoc.ReadOnly(), xdoc.WithPassword(m.opts.RegisterCode))
		if readErr == nil {
			_ = ro.Close()
		} else if strings.Contains(readErr.Error(), notEncryptedMarker) {
			stateDB, err = m.fallbackToEmpty(statePath, stateFileName, "", readErr)
			if err != nil {
				_ = identityDB.Close()
				return nil, nil, err
			}
			return identityDB, stateDB, nil
		}
	}

	stateDB, err = xdoc.Open(statePath, xdoc.WithPassword(m.opts.RegisterCode))
	switch {
	case err == nil:
		return identityDB, stateDB, nil
	case errors.Is(err, xdoc.ErrNeedsRebuild), isInvalidPasswordError(err), err != nil && strings.Contains(err.Error(), notEncryptedMarker):
		// identity.xdoc 已经证明口令正确：state.xdoc 的 ErrNeedsRebuild /
		// INVALID_PASSWORD/NOT_ENCRYPTED indicate unusable encrypted state.
		// The verified identity allows rebuilding it without discarding identity.
		stateDB, err = m.rebuildFile(ctx, statePath, stateFileName)
		if err != nil {
			_ = identityDB.Close()
			return nil, nil, err
		}
		return identityDB, stateDB, nil
	default:
		_ = identityDB.Close()
		return nil, nil, fmt.Errorf("store: 打开 state.xdoc 失败: %w", err)
	}
}

// rebuildFile creates a read-only backup before rebuilding recoverable records.
// Identity must be verified before any state recovery writes occur.
func (m *Module) rebuildFile(ctx context.Context, path, label string) (*xdoc.DB, error) {
	slog.Warn("local state store needs rebuild, starting automatic rebuild", slog.String("file", label))

	backupPath, berr := m.backupBeforeRebuild(ctx, path, label)
	if berr != nil {

		slog.Error("pre-rebuild supplementary read-only backup failed, continuing rebuild",
			slog.String("file", label), slog.Any("error", berr))
	}

	result, rerr := xdoc.Rebuild(path, xdoc.WithPassword(m.opts.RegisterCode))
	if rerr != nil {
		return m.fallbackToEmpty(path, label, backupPath, rerr)
	}
	if len(result.Skipped) > 0 {
		slog.Warn("local state store rebuilt with some data loss",
			slog.String("file", label), slog.Int("skipped", len(result.Skipped)),
			slog.Any("salvaged_collections", result.Salvaged))
		m.recordRebuild(label, "WARN", fmt.Sprintf(
			"自动重建成功，但有 %d 个集合无法恢复（已抢救集合：%v），原文件的只读备份见 BackupPath",
			len(result.Skipped), result.Salvaged), backupPath)
	} else {
		slog.Info("local state store rebuilt successfully", slog.String("file", label))
		m.recordRebuild(label, "INFO", "自动重建成功，未发现数据丢失", backupPath)
	}

	db, err := xdoc.Open(path, xdoc.WithPassword(m.opts.RegisterCode))
	if err != nil {
		return nil, fmt.Errorf("store: %s 重建后重新打开失败: %w", label, err)
	}
	return db, nil
}

// backupBeforeRebuild 只读打开可疑文件、导出一份独立备份。只读打开对任何
// 可疑文件（含需要重建的）都是安全的（已勘察确认：只读模式不碰文件一个字节）。
// 成功时返回导出的备份文件路径，供调用方经 recordRebuild 记入
// RebuildRecord.BackupPath；失败时返回空字符串。
func (m *Module) backupBeforeRebuild(ctx context.Context, path, label string) (string, error) {
	ro, err := xdoc.Open(path, xdoc.ReadOnly(), xdoc.WithPassword(m.opts.RegisterCode))
	if err != nil {
		return "", fmt.Errorf("只读打开失败: %w", err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(ro.Close)

	backupDir := filepath.Join(m.opts.DataDir, backupDirName)
	if err := os.MkdirAll(backupDir, 0o750); err != nil {
		return "", fmt.Errorf("创建 backup 目录失败: %w", err)
	}
	dst := filepath.Join(backupDir, fmt.Sprintf("%s.%s.bak", label, m.now().Format("20060102-150405.000")))
	if err := ro.BackupFile(ctx, dst); err != nil {
		return "", fmt.Errorf("导出只读备份失败: %w", err)
	}
	return dst, nil
}

func (m *Module) fallbackToEmpty(path, label, backupPath string, rebuildErr error) (*xdoc.DB, error) {
	corruptPath, err := m.moveToCorrupt(path, label)
	if err != nil {
		m.recordRebuild(label, "ERROR", fmt.Sprintf(
			"自动重建失败且无法移入 corrupt/：rebuild=%v, move=%v", rebuildErr, err), backupPath)
		return nil, fmt.Errorf("store: %s 重建失败且无法移入 corrupt/: rebuild=%w, move=%v", label, rebuildErr, err)
	}
	slog.Error("local state store rebuild failed",
		slog.String("file", label), slog.Any("rebuild_error", rebuildErr))
	m.recordRebuild(label, "ERROR", fmt.Sprintf(
		"自动重建失败，已降级为空状态启动；原文件已移至 corrupt/ 供人工抢救：%v", rebuildErr), corruptPath)

	db, err := xdoc.Open(path, xdoc.WithPassword(m.opts.RegisterCode))
	if err != nil {
		return nil, fmt.Errorf("store: %s 重建失败后以空状态重新创建也失败: %w", label, err)
	}
	return db, nil
}

func (m *Module) moveToCorrupt(path, label string) (string, error) {
	corruptDir := filepath.Join(m.opts.DataDir, corruptDirName)
	if err := os.MkdirAll(corruptDir, 0o750); err != nil {
		return "", fmt.Errorf("创建 corrupt 目录失败: %w", err)
	}
	dst := filepath.Join(corruptDir, fmt.Sprintf("%s.%s", label, m.now().Format("20060102-150405.000")))
	// Preserve bytes in an independent file before replacing the old path.
	// A failed encrypted open can retain a file handle, so a renamed inode must
	// not be the only backup while a new database is opened at the old path.
	source, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func(cleanup func() error) { _ = cleanup() }(source.Close)
	backup, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(backup, source)
	if copyErr == nil {
		copyErr = backup.Sync()
	}
	closeErr := backup.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", fmt.Errorf("备份 %s 到 corrupt/ 失败: %w", label, err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("备份后移除 %s 失败: %w", label, err)
	}
	return dst, nil
}
