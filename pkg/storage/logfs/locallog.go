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

package logfs

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/intervals"
	"github.com/solarisdb/solaris/pkg/ql"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/chunkfs"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"time"
)

type (
	// localLog implements Log interface for working with data stored in the chunks on the local file-system
	localLog struct {
		LMStorage    LogsMetaStorage   `inject:""`
		ChnkProvider *chunkfs.Provider `inject:""`

		cfg     Config
		logger  logging.Logger
		lockers *lru.ReleasableCache[string, *logLocker]
	}

	logLocker struct {
		lock sync.Mutex
	}

	// LogsMetaStorage interface describes a log meata storage for the log chunks info
	LogsMetaStorage interface {
		// GetLastChunk returns the chunk with the biggest chunkID
		GetLastChunk(ctx context.Context, logID string) (ChunkInfo, error)
		// GetChunks returns the list of chunks associated with the logID
		GetChunks(ctx context.Context, logID string) ([]ChunkInfo, error)
		// UpsertChunkInfos update or insert new records associated with logID into the meta-storage
		UpsertChunkInfos(ctx context.Context, logID string, cis []ChunkInfo) error
	}

	// ChunkInfo is the descriptor which describes a chunk information in the log meta-storage
	ChunkInfo struct {
		// ID is the chunk ID
		ID string `json:"id"`
		// Min is the minimum (first) record ID stored in the chunk
		Min ulid.ULID `json:"min"`
		// Max is the maximum (last) record ID stored in the chunk
		Max ulid.ULID `json:"max"`
		// RecordsCount is the number of records stored in the chunk
		RecordsCount int `json:"recordsCount"`
	}

	idRange struct {
		start ulid.ULID
		end   ulid.ULID
	}
)

const (
	// ChunkMinID defines the lower boundary for chunk ID (exclusive)
	ChunkMinID = ""
	// ChunkMaxID defines the upper boundary for chunk ID (exclusive)
	ChunkMaxID = "~"
)

var _ storage.Log = (*localLog)(nil)

var (
	tiBasis   = intervals.BasisTime
	tiBuilder = ql.NewParamIntervalBuilder(tiBasis, ql.RecordsCondValueDialect, "ctime", ql.OpsAll)
)

// NewLocalLog creates the new localLog object for the cfg provided
func NewLocalLog(cfg Config) *localLog {
	l := new(localLog)
	l.cfg = cfg
	l.logger = logging.NewLogger("localLog")
	var err error
	l.lockers, err = lru.NewReleasableCache[string, *logLocker](cfg.MaxLocks,
		func(ctx context.Context, lid string) (*logLocker, error) {
			return &logLocker{}, nil
		}, nil)
	if err != nil {
		panic(err)
	}
	return l
}

// Shutdown implements linker.Shutdowner
func (l *localLog) Shutdown() {
	l.logger.Infof("Shutting down.")
	l.lockers.Close()
}

// AppendRecords allows to write reocrds into the chunks on the local FS and update the Logs catalog with the new
// chunks created
func (l *localLog) AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error) {
	lid := request.LogID
	ll, err := l.lockers.GetOrCreate(ctx, lid)
	if err != nil {
		return nil, fmt.Errorf("could not obtain the log locker for id=%s: %w", lid, err)
	}
	defer l.lockers.Release(&ll)
	ll.Value().lock.Lock()
	defer ll.Value().lock.Unlock()

	cis := []ChunkInfo{}

	ci, err := l.LMStorage.GetLastChunk(ctx, lid)
	if err != nil && !errors.Is(err, errors.ErrNotExist) {
		return nil, err
	}

	recs := request.Records
	added := 0
	var gerr error
	for len(recs) > 0 {
		if ci.RecordsCount == 0 {
			ci = ChunkInfo{ID: ulidutils.NewID()}
			l.logger.Infof("creating new chunk id=%s for the logID=%s", ci.ID, lid)
		}
		arr, err := l.appendRecords(ctx, ci.ID, ci.RecordsCount == 0, recs)
		if err != nil {
			gerr = err
			break
		}
		if arr.Written > 0 {
			if ci.RecordsCount == 0 {
				ci.Min = arr.StartID
			}
			ci.Max = arr.LastID
			ci.RecordsCount += arr.Written
			cis = append(cis, ci)
			recs = recs[arr.Written:]
			added += arr.Written
		} else if ci.RecordsCount == 0 {
			// the chunk was just created and its capacity is not enough to write at least one record!
			gerr = fmt.Errorf("it seems the maximum chunk size is less than the record size payload=%d: %w", len(recs[0].Payload), errors.ErrInvalid)
			break
		}
		ci.RecordsCount = 0
	}

	if ci.RecordsCount == 0 {
		l.ChnkProvider.DeleteFileIfEmpty(ci.ID)
	}

	if added > 0 {
		// use context.Background instead of ctx to avoid some unrecoverable error in case of the ctx is closed, but we have some
		// data written
		if err := l.LMStorage.UpsertChunkInfos(ctx, lid, cis); err != nil {
			// well, now it is unrecoverable!
			l.logger.Errorf("could not write chunk IDs=%v for logID=%s, but the data is written into chunk. The data is corrupted now: %v", cis, lid, err)
			panic("unrecoverable error, data is corrupted")
		}
		if gerr != nil {
			l.logger.Warnf("AppendRecords: got the error=%v, but would be able to write some data for logID=%s, added=%d", gerr, lid, added)
		}
		gerr = nil // disregard the error, cause we could write something
	}

	response := &solaris.AppendRecordsResult{Added: int64(added)}
	if request.ExpandIDs {
		ids := make([]string, added)
		for idx := 0; idx < added; idx++ {
			ids[idx] = request.Records[idx].ID
		}
		response.RecordIDs = ids
	}

	return response, gerr
}

func (l *localLog) appendRecords(ctx context.Context, cID string, newFile bool, recs []*solaris.Record) (chunkfs.AppendRecordsResult, error) {
	rc, err := l.ChnkProvider.GetOpenedChunk(ctx, cID, newFile)
	if err != nil {
		return chunkfs.AppendRecordsResult{}, err
	}
	defer l.ChnkProvider.ReleaseChunk(&rc)

	// request write access to the chunk
	if err := l.ChnkProvider.CA.SetWriting(ctx, cID); err != nil {
		return chunkfs.AppendRecordsResult{}, err
	}
	defer l.ChnkProvider.CA.SetIdle(cID)

	return rc.Value().AppendRecords(recs)
}

// QueryRecords allows to retrieve records from the Log by its ID. The function will control the limit of the result. If
// the number of records or the cumulative payload size hit the limits the function may return fewer records than requested
// or available. The second return parameters returns whether there are potentially more records than requested.
func (l *localLog) QueryRecords(ctx context.Context, request storage.QueryRecordsRequest) ([]*solaris.Record, bool, error) {
	lid := request.LogID

	// the l.lockers plays a role of limiter as well, it doesn't allow to have more than N locks available,
	// so the l.lockers.GetOrCreate(ctx, lid) will be blocked if number of requested locks (not the number of requests!)
	// exceeds the maximum (N) capacity.
	// We will request the lock for supporting the limited number of logs in a work a time, but will not to Lock it for
	// the read operation. Only AppendRecords does this to support its atomicy.
	ll, err := l.lockers.GetOrCreate(ctx, lid)
	if err != nil {
		return nil, false, fmt.Errorf("could not obtain the log locker for id=%s: %w", lid, err)
	}
	defer l.lockers.Release(&ll)

	cis, err := l.LMStorage.GetChunks(ctx, lid)
	if err != nil {
		return nil, false, err
	}
	if len(cis) == 0 {
		return nil, false, nil
	}

	var fromIdx int
	inc := 1
	if request.Descending {
		inc = -1
		fromIdx = len(cis) - 1
	}

	var sid ulid.ULID
	if request.StartID != "" {
		if err = sid.UnmarshalText(cast.StringToByteArray(request.StartID)); err != nil {
			l.logger.Warnf("could not unmarshal startID=%s: %v", request.StartID, err)
			return nil, false, fmt.Errorf("wrong startID=%q: %w", request.StartID, errors.ErrInvalid)
		}
		if request.Descending {
			fromIdx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Min.Compare(sid) > 0
			})
			fromIdx--
			inc = -1
		} else {
			fromIdx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Max.Compare(sid) >= 0
			})
		}
	}

	tis, err := getIntervals(request.Condition)
	if err != nil {
		return nil, false, err
	}
	if len(request.Condition) > 0 && len(tis) == 0 {
		return nil, false, nil
	}

	limit := int(request.Limit)
	if limit > l.cfg.MaxRecordsLimit {
		limit = l.cfg.MaxRecordsLimit
	}
	totalSize := 0

	var res []*solaris.Record
	for idx := fromIdx; idx >= 0 && idx < len(cis) && limit > len(res); idx += inc {
		ci := cis[idx]
		idRanges := getRanges(tis, ci)
		if len(request.Condition) > 0 && len(idRanges) == 0 {
			continue
		}
		srecs, err := l.readRecords(ctx, lid, ci, request.Descending, considerSIDAndDesc(idRanges, sid, request.Descending), limit-len(res), &totalSize)
		if err != nil {
			return nil, false, err
		}
		res = append(res, srecs...)
		sid = ulidutils.ZeroULID
	}
	return res, len(res) >= limit || totalSize >= l.cfg.MaxBunchSize, nil
}

// CountRecords count total number for records in the log and number of records after (before)
// specified record ID which match the request condition. Returned values are (total, count, error).
func (l *localLog) CountRecords(ctx context.Context, request storage.QueryRecordsRequest) (uint64, uint64, error) {
	lid := request.LogID

	// the l.lockers plays a role of limiter as well, it doesn't allow to have more than N locks available,
	// so the l.lockers.GetOrCreate(ctx, lid) will be blocked if number of requested locks (not the number of requests!)
	// exceeds the maximum (N) capacity.
	// We will request the lock for supporting the limited number of logs in a work a time, but will not to Lock it for
	// the read operation. Only AppendRecords does this to support its atomicy.
	ll, err := l.lockers.GetOrCreate(ctx, lid)
	if err != nil {
		return 0, 0, fmt.Errorf("could not obtain the log locker for id=%s: %w", lid, err)
	}
	defer l.lockers.Release(&ll)

	cis, err := l.LMStorage.GetChunks(ctx, lid)
	if err != nil {
		return 0, 0, err
	}
	if len(cis) == 0 {
		return 0, 0, nil
	}

	var initIdx int
	var fromIdx int

	inc := 1
	if request.Descending {
		inc = -1
		initIdx = len(cis) - 1
		fromIdx = len(cis) - 1
	}

	var sid ulid.ULID
	if request.StartID != "" {
		if err = sid.UnmarshalText(cast.StringToByteArray(request.StartID)); err != nil {
			l.logger.Warnf("could not unmarshal startID=%s: %v", request.StartID, err)
			return 0, 0, fmt.Errorf("wrong startID=%q: %w", request.StartID, errors.ErrInvalid)
		}
		if request.Descending {
			fromIdx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Min.Compare(sid) > 0
			})
			fromIdx--
		} else {
			fromIdx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Max.Compare(sid) >= 0
			})
		}
	}

	tis, err := getIntervals(request.Condition)
	if err != nil {
		return 0, 0, err
	}
	if len(request.Condition) > 0 && len(tis) == 0 {
		return 0, 0, nil
	}

	var total uint64
	var count uint64

	for idx := initIdx; idx >= 0 && idx < len(cis); idx += inc {
		ci := cis[idx]
		total += uint64(ci.RecordsCount)
		if (request.Descending && idx <= fromIdx) || (!request.Descending && idx >= fromIdx) {
			idRanges := getRanges(tis, ci)
			if len(request.Condition) > 0 && len(idRanges) == 0 {
				continue
			}
			recCnt := uint64(ci.RecordsCount)
			if sid.Compare(ulidutils.ZeroULID) != 0 || len(idRanges) > 0 {
				recCnt, err = l.countRecords(ctx, ci, request.Descending, considerSIDAndDesc(idRanges, sid, request.Descending))
				if err != nil {
					return 0, 0, nil
				}
			}
			count += recCnt
			sid = ulidutils.ZeroULID
		}
	}

	return total, count, nil
}

func (l *localLog) readRecords(
	ctx context.Context,
	lid string,
	ci ChunkInfo,
	desc bool,
	idRanges []idRange,
	limit int,
	totalSize *int) ([]*solaris.Record, error) {
	rc, err := l.ChnkProvider.GetOpenedChunk(ctx, ci.ID, false)
	if err != nil {
		return nil, err
	}
	defer l.ChnkProvider.ReleaseChunk(&rc)

	cr, err := rc.Value().OpenChunkReader(desc)
	if err != nil {
		return nil, err
	}
	defer cr.Close()

	var res []*solaris.Record
	for _, ir := range idRanges {
		if ir.start.Compare(ulidutils.ZeroULID) != 0 {
			cr.SetStartID(ir.start)
		}
		for cr.HasNext() && len(res) < limit && *totalSize < l.cfg.MaxBunchSize {
			ur, _ := cr.Next()
			if ir.end.Compare(ulidutils.ZeroULID) != 0 &&
				((desc && ur.ID.Compare(ir.end) < 0) || (!desc && ur.ID.Compare(ir.end) > 0)) {
				break
			}
			r := new(solaris.Record)
			r.ID = ur.ID.String()
			r.LogID = lid
			r.Payload = make([]byte, len(ur.UnsafePayload))
			copy(r.Payload, ur.UnsafePayload)
			r.CreatedAt = timestamppb.New(ulid.Time(ur.ID.Time()))
			*totalSize += len(ur.UnsafePayload)
			res = append(res, r)
		}
	}
	return res, nil
}

func (l *localLog) countRecords(ctx context.Context,
	ci ChunkInfo,
	desc bool,
	idRanges []idRange) (uint64, error) {

	rc, err := l.ChnkProvider.GetOpenedChunk(ctx, ci.ID, false)
	if err != nil {
		return 0, err
	}
	defer l.ChnkProvider.ReleaseChunk(&rc)

	cr, err := rc.Value().OpenChunkReader(desc)
	if err != nil {
		return 0, err
	}
	defer cr.Close()

	var count uint64
	for _, ir := range idRanges {
		if ir.start.Compare(ulidutils.ZeroULID) != 0 {
			cr.SetStartID(ir.start)
		}
		for cr.HasNext() {
			ur, _ := cr.Next()
			if ir.end.Compare(ulidutils.ZeroULID) != 0 &&
				((desc && ur.ID.Compare(ir.end) < 0) || (!desc && ur.ID.Compare(ir.end) > 0)) {
				break
			}
			count++
		}
	}
	return count, nil
}

func getIntervals(cond string) ([]intervals.Interval[time.Time], error) {
	if len(strings.TrimSpace(cond)) == 0 {
		return nil, nil
	}
	expr, err := ql.Parse(cond)
	if err != nil {
		return nil, err
	}
	tis, err := tiBuilder.Build(expr)
	if err != nil {
		return nil, err
	}
	return tis, nil
}

func getRanges(tis []intervals.Interval[time.Time], ci ChunkInfo) []idRange {
	cti := tiBasis.Closed(ulid.Time(ci.Min.Time()), ulid.Time(ci.Max.Time()))
	var ranges []idRange
	for _, ti := range tis {
		if ri, ok := tiBasis.Intersect(cti, ti); ok {
			ranges = append(ranges, toRange(ri))
		}
	}
	return ranges
}

func toRange(ti intervals.Interval[time.Time]) idRange {
	if ti.IsClosed() {
		return idRange{start: minULIDForTime(ti.L), end: maxULIDForTime(ti.R)}
	} else if ti.IsOpenL() {
		return idRange{start: minULIDForTime(ti.L.Add(time.Millisecond)), end: maxULIDForTime(ti.R)}
	} else if ti.IsOpenR() {
		return idRange{start: minULIDForTime(ti.L), end: maxULIDForTime(ti.R.Add(-time.Millisecond))}
	} else { // open
		return idRange{start: minULIDForTime(ti.L.Add(time.Millisecond)), end: maxULIDForTime(ti.R.Add(-time.Millisecond))}
	}
}

func minULIDForTime(t time.Time) ulid.ULID {
	var id ulid.ULID
	_ = id.SetTime(uint64(t.UnixMilli()))
	return id
}

func maxULIDForTime(t time.Time) ulid.ULID {
	maxBytes := make([]byte, 10)
	for i := 0; i < len(maxBytes); i++ {
		maxBytes[i] = 0xff
	}
	var id ulid.ULID
	_ = id.SetTime(uint64(t.UnixMilli()))
	_ = id.SetEntropy(maxBytes)
	return id
}

func considerSIDAndDesc(irs []idRange, sid ulid.ULID, desc bool) []idRange {
	if len(irs) == 0 {
		return []idRange{{start: sid}}
	}
	if sid.Compare(ulidutils.ZeroULID) == 0 {
		if desc {
			return reverseRanges(irs)
		}
		return irs
	}
	if desc {
		irs = reverseRanges(irs)
		if sid.Compare(irs[0].start) >= 0 {
			return irs
		}
		if sid.Compare(irs[len(irs)-1].end) < 0 {
			return nil
		}
		for i, r := range irs {
			if r.end.Compare(sid) <= 0 {
				if r.start.Compare(sid) > 0 {
					irs[i].start = sid
				}
				irs = irs[i:]
				break
			}
		}
		return irs
	}
	if sid.Compare(irs[0].start) <= 0 {
		return irs
	}
	if sid.Compare(irs[len(irs)-1].end) > 0 {
		return nil
	}
	for i, r := range irs {
		if r.end.Compare(sid) >= 0 {
			if r.start.Compare(sid) < 0 {
				irs[i].start = sid
			}
			irs = irs[i:]
			break
		}
	}
	return irs
}

func reverseRanges(irs []idRange) []idRange {
	if len(irs) == 0 {
		return irs
	}
	l := 0
	r := len(irs) - 1
	for l < r {
		irs[l].start, irs[l].end = irs[l].end, irs[l].start
		irs[r].start, irs[r].end = irs[r].end, irs[r].start
		irs[l], irs[r] = irs[r], irs[l]
		l++
		r--
	}
	if len(irs)&1 != 0 {
		irs[l].start, irs[l].end = irs[l].end, irs[l].start
	}
	return irs
}
