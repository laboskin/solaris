// Copyright 2024 The Solaris Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/ql"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"strings"
	"time"
)

// Storage is the logs meta storage
type Storage struct {
	db *Db
}

// qlToPqTranslator is the logs condition translator from solaris QL to the Postgres dialect
var qlToPqTranslator = ql.NewTranslator(ql.LogsCondTranslateDialect)

// NewStorage creates new logs meta storage based on Postgres
func NewStorage(db *Db) *Storage {
	return &Storage{db: db}
}

// ===================================== logs =====================================

// CreateLog implements storage.Logs
func (s *Storage) CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	newLog := logToModel(log)
	newLog.ID = ulidutils.NewID()
	newLog.CreatedAt = time.Now()
	newLog.UpdatedAt = newLog.CreatedAt

	_, err := s.db.ExecContext(ctx, "insert into log (id, tags, records, created_at, updated_at) values ($1, $2, $3, $4, $5)",
		newLog.ID, newLog.Tags.JSON(), newLog.Records, newLog.CreatedAt, newLog.UpdatedAt)
	if err != nil {
		return nil, MapError(err)
	}
	return logToAPI(newLog), nil
}

// GetLogByID implements storage.Logs
func (s *Storage) GetLogByID(ctx context.Context, id string) (*solaris.Log, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("ID must be specified: %w", errors.ErrInvalid)
	}
	var log Log
	if err := s.db.GetContext(ctx, &log, "select * from log where id=$1 and deleted = false", id); err != nil {
		return nil, MapError(err)
	}
	return logToAPI(log), nil
}

// UpdateLog implements storage.Logs
func (s *Storage) UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	if len(log.ID) == 0 {
		return nil, fmt.Errorf("log ID must be specified: %w", errors.ErrInvalid)
	}
	rows, err := s.db.QueryxContext(ctx, "update log set tags = $1, updated_at = $2 where id = $3 and deleted = false returning *",
		Tags(log.Tags).JSON(), time.Now(), log.ID)
	if err != nil {
		return nil, MapError(err)
	}
	defer func() {
		_ = rows.Close()
	}()
	if !rows.Next() {
		return nil, errors.ErrNotExist
	}
	var updatedLog Log
	if err = rows.StructScan(&updatedLog); err != nil {
		return nil, MapError(err)
	}
	return logToAPI(updatedLog), nil
}

// QueryLogs implements storage.Logs
func (s *Storage) QueryLogs(ctx context.Context, qr storage.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	var sb strings.Builder
	args := make([]interface{}, 0)

	if len(qr.IDs) > 0 {
		sb.WriteString(" id in (")
		for _, id := range qr.IDs {
			if len(args) > 0 {
				sb.WriteString(", ")
			}
			args = append(args, id)
			sb.WriteString(fmt.Sprintf("$%d", len(args)))
		}
		sb.WriteString(")")
	} else if len(qr.Condition) > 0 {
		if err := qlToPqTranslator.Translate(&sb, qr.Condition); err != nil {
			return nil, fmt.Errorf("condition=%q translate error=%v: %w", qr.Condition, err, errors.ErrInvalid)
		}
	}

	if sb.Len() > 0 {
		sb.WriteString(" and ")
	}
	args = append(args, qr.Deleted)
	sb.WriteString(fmt.Sprintf("deleted = $%d", len(args)))

	var where string
	if sb.Len() > 0 {
		where = "where " + sb.String()
	}

	cntRows, err := s.db.QueryxContext(ctx, fmt.Sprintf("select count(*) from log %s ", where), args...)
	if err != nil {
		return nil, MapError(err)
	}
	defer func() {
		_ = cntRows.Close()
	}()
	var count int64
	if count, err = scan[int64](cntRows); err != nil {
		return nil, MapError(err)
	}

	limit := int(qr.Limit)
	args = append(args, limit+1)

	qryRows, err := s.db.QueryxContext(ctx, fmt.Sprintf("select * from log %s order by id limit $%d", where, len(args)), args...)
	if err != nil {
		return nil, MapError(err)
	}
	defer func() {
		_ = qryRows.Close()
	}()
	logs, err := structScanRows[Log](qryRows)
	if err != nil {
		return nil, MapError(err)
	}

	var nextPageID string
	if len(logs) > limit {
		nextPageID = logs[limit].ID
		logs = logs[:limit]
	}
	return &solaris.QueryLogsResult{
		Logs:       logsToAPI(logs),
		NextPageID: nextPageID,
		Total:      count,
	}, nil
}

// DeleteLogs implements storage.Logs
func (s *Storage) DeleteLogs(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.DeleteLogsResult, error) {
	var sb strings.Builder
	args := make([]interface{}, 0)

	if len(req.IDs) > 0 {
		sb.WriteString(" id in (")
		for _, id := range req.IDs {
			if len(args) > 0 {
				sb.WriteString(", ")
			}
			args = append(args, id)
			sb.WriteString(fmt.Sprintf("$%d", len(args)))
		}
		sb.WriteString(")")
	} else if len(req.Condition) > 0 {
		if err := qlToPqTranslator.Translate(&sb, req.Condition); err != nil {
			return nil, fmt.Errorf("condition=%q translate error=%v: %w", req.Condition, err, errors.ErrInvalid)
		}
	}
	if sb.Len() == 0 {
		return &solaris.DeleteLogsResult{}, nil
	}

	var err error
	var rows *sqlx.Rows

	if req.MarkOnly {
		if sb.Len() > 0 {
			sb.WriteString(" and ")
		}
		args = append(args, false)
		sb.WriteString(fmt.Sprintf(" deleted = $%d ", len(args)))
		rows, err = s.db.QueryxContext(ctx, fmt.Sprintf("update log set deleted = true where %s returning id", sb.String()), args...)
	} else {
		rows, err = s.db.QueryxContext(ctx, fmt.Sprintf("delete from log where %s returning id", sb.String()), args...)
	}
	if err != nil {
		return nil, MapError(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	ids, err := scanRows[string](rows)
	if err != nil {
		return nil, MapError(err)
	}
	return &solaris.DeleteLogsResult{DeletedIDs: ids}, nil
}

// ===================================== chunks =====================================

// GetLastChunk implements logfs.LogsMetaStorage
func (s *Storage) GetLastChunk(ctx context.Context, logID string) (logfs.ChunkInfo, error) {
	if len(logID) == 0 {
		return logfs.ChunkInfo{}, fmt.Errorf("log ID must be specified: %w", errors.ErrInvalid)
	}
	var chunk Chunk
	if err := s.db.GetContext(ctx, &chunk, "select * from chunk where log_id=$1 order by id desc limit 1", logID); err != nil {
		return logfs.ChunkInfo{}, MapError(err)
	}
	return chunkToInfo(chunk), nil
}

// GetChunks implements logfs.LogsMetaStorage
func (s *Storage) GetChunks(ctx context.Context, logID string) ([]logfs.ChunkInfo, error) {
	if len(logID) == 0 {
		return nil, fmt.Errorf("log ID must be specified: %w", errors.ErrInvalid)
	}
	rows, err := s.db.QueryxContext(ctx, "select * from chunk where log_id = $1 order by id", logID)
	if err != nil {
		return nil, MapError(err)
	}
	defer func() {
		_ = rows.Close()
	}()
	logs, err := structScanRows[Chunk](rows)
	if err != nil {
		return nil, MapError(err)
	}
	return chunksToInfo(logs), nil
}

// UpsertChunkInfos implements logfs.LogsMetaStorage
func (s *Storage) UpsertChunkInfos(ctx context.Context, logID string, cis []logfs.ChunkInfo) error {
	if len(logID) == 0 {
		return fmt.Errorf("log ID must be specified: %w", errors.ErrInvalid)
	}
	if len(cis) == 0 {
		return nil
	}

	var sb strings.Builder
	var args []any

	firstIdx := 1
	sb.WriteString("insert into chunk (id, log_id, min, max, records) values ")

	for i, ci := range cis {
		if len(ci.ID) == 0 {
			return fmt.Errorf("chunk ID for item=%d  must be specified: %w", i, errors.ErrInvalid)
		}
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", firstIdx, firstIdx+1, firstIdx+2, firstIdx+3, firstIdx+4))
		firstIdx += 5
		args = append(args, ci.ID)
		args = append(args, logID)
		args = append(args, ci.Min.String())
		args = append(args, ci.Max.String())
		args = append(args, ci.RecordsCount)
	}

	sb.WriteString(" on conflict (id, log_id) do update set (min, max, records) = (excluded.min, excluded.max, excluded.records)")
	_, err := s.db.ExecContext(ctx, sb.String(), args...)
	return MapError(err)
}

// ===================================== helpers =====================================

func scan[T any](rows *sqlx.Rows) (T, error) {
	var res T
	if rows.Next() {
		_ = rows.Scan(&res)
	}
	return res, nil
}

func scanRows[T any](rows *sqlx.Rows) ([]T, error) {
	var res []T
	for rows.Next() {
		var t T
		if err := rows.Scan(&t); err != nil {
			return nil, MapError(err)
		}
		res = append(res, t)
	}
	return res, nil
}

func structScanRows[T any](rows *sqlx.Rows) ([]T, error) {
	var res []T
	for rows.Next() {
		var t T
		if err := rows.StructScan(&t); err != nil {
			return nil, MapError(err)
		}
		res = append(res, t)
	}
	return res, nil
}
