package postgres

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"maps"
	"testing"
)

type testSuite struct {
	pgTestSuite
}

func TestRunTestSuite(t *testing.T) {
	suite.Run(t, &testSuite{pgTestSuite{}})
}

func (ts *testSuite) Test_CreateLog() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(ts.T(), err)
	assert.NotEmpty(ts.T(), log.ID)
	assert.NotEmpty(ts.T(), log.CreatedAt)
	assert.NotEmpty(ts.T(), log.UpdatedAt)
}

func (ts *testSuite) Test_UpdateLog() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2, err := s.GetLogByID(ctx, log1.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), log2.ID, log1.ID)
	assert.True(ts.T(), maps.Equal(log2.Tags, log1.Tags))

	log1.Tags["tag5"] = "val5"
	log2, err = s.UpdateLog(ctx, log1)
	assert.Nil(ts.T(), err)
	assert.True(ts.T(), maps.Equal(log2.Tags, log1.Tags))
}

func (ts *testSuite) Test_GetLogByID() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	log3, err := s.GetLogByID(ctx, log2.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), log2.ID, log3.ID)
	assert.True(ts.T(), maps.Equal(log2.Tags, log3.Tags))

	log4, err := s.GetLogByID(ctx, log1.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), log1.ID, log4.ID)
	assert.True(ts.T(), maps.Equal(log1.Tags, log4.Tags))
}

func (ts *testSuite) Test_QueryLogsByCondition() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(ts.T(), err)

	log4 := &solaris.Log{Tags: map[string]string{"a": "b"}}
	log4, err = s.CreateLog(ctx, log4)
	assert.Nil(ts.T(), err)

	qr, err := s.QueryLogs(ctx, storage.QueryLogsRequest{Condition: "tag('tag3') = 'val3' OR tag('tag3') = 'val4' OR tag('tag1') like 'v%1'", Limit: 2})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 2, len(qr.Logs))
	assert.Equal(ts.T(), int64(3), qr.Total)
	assert.Equal(ts.T(), qr.NextPageID, log3.ID)
}

func (ts *testSuite) Test_QueryLogsByIDs() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	log3 := &solaris.Log{}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(ts.T(), err)

	qr, err := s.QueryLogs(ctx, storage.QueryLogsRequest{IDs: []string{log1.ID, log2.ID, log3.ID}, Condition: "must not matter", Limit: 2})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 2, len(qr.Logs))
	assert.Equal(ts.T(), int64(3), qr.Total)
	assert.Equal(ts.T(), qr.NextPageID, log3.ID)
}

func (ts *testSuite) Test_DeleteLogsByCondition() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(ts.T(), err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag3') = 'val4' AND tag('tag4') like 'v%'"})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 1, len(dr.DeletedIDs))
}

func (ts *testSuite) Test_DeleteLogsByConditionMarkOnly() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(ts.T(), err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'", MarkOnly: true})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 1, len(dr.DeletedIDs))

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'", MarkOnly: true})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 0, len(dr.DeletedIDs))

	log, err = s.GetLogByID(ctx, log.ID)
	assert.ErrorIs(ts.T(), err, errors.ErrNotExist)

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'"})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 1, len(dr.DeletedIDs))

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'"})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 0, len(dr.DeletedIDs))
}

func (ts *testSuite) Test_DeleteLogsByIDs() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(ts.T(), err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID, log3.ID}})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 2, len(dr.DeletedIDs))
}

func (ts *testSuite) Test_DeleteLogByIDsWithChunks() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log1.ID, cis1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	cis2 := []logfs.ChunkInfo{{ID: "3"}, {ID: "4"}}
	err = s.UpsertChunkInfos(ctx, log2.ID, cis2)
	assert.Nil(ts.T(), err)

	cis3, err := s.GetChunks(ctx, log1.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis1), len(cis3))

	cis4, err := s.GetChunks(ctx, log2.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis2), len(cis4))

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID}})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 1, len(dr.DeletedIDs))

	cis3, err = s.GetChunks(ctx, log1.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis1), len(cis3))

	cis4, err = s.GetChunks(ctx, log2.ID)
	assert.Equal(ts.T(), 0, len(cis4))
}

func (ts *testSuite) Test_GetLastChunk() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	_, err := s.GetLastChunk(ctx, "noID")
	assert.ErrorIs(ts.T(), err, errors.ErrNotExist)

	log := &solaris.Log{}
	log, err = s.CreateLog(ctx, log)
	assert.Nil(ts.T(), err)

	_, err = s.GetLastChunk(ctx, log.ID)
	assert.ErrorIs(ts.T(), err, errors.ErrNotExist)

	cis := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis)
	assert.Nil(ts.T(), err)

	ci, err := s.GetLastChunk(ctx, log.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), cis[0].ID, ci.ID)
}

func (ts *testSuite) Test_GetChunks() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log := &solaris.Log{}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(ts.T(), err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis1)
	assert.Nil(ts.T(), err)

	cis2, err := s.GetChunks(ctx, log.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis1), len(cis2))
}

func (ts *testSuite) Test_UpsertChunkInfos() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log := &solaris.Log{}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(ts.T(), err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis1)
	assert.Nil(ts.T(), err)

	cis2, err := s.GetChunks(ctx, log.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis1), len(cis2))

	cis3 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}, {ID: "3"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis3)
	assert.Nil(ts.T(), err)

	cis4, err := s.GetChunks(ctx, log.ID)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), len(cis3), len(cis4))
}

func (ts *testSuite) Test_DeleteLogChunks() {
	ctx := context.Background()
	s := NewStorage(ts.db)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(ts.T(), err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log1.ID, cis1)
	assert.Nil(ts.T(), err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(ts.T(), err)

	cis2 := []logfs.ChunkInfo{{ID: "3"}, {ID: "4"}}
	err = s.UpsertChunkInfos(ctx, log2.ID, cis2)
	assert.Nil(ts.T(), err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID}})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), 1, len(dr.DeletedIDs))
}
