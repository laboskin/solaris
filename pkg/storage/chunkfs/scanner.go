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

package chunkfs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logrange/linker"
	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type (
	// ScannerConfig defines settings for the Scanner for replicating chunks from the local file-system to the remote Storage
	ScannerConfig struct {
		// DataPath contains the path to the folder where the chunks are stored
		DataPath string
		// SweepMaxThresholdSize defines the maximum value of the local chunks' folder size when the sweeper
		// starts to remove chunks from the local file-system
		SweepMaxThresholdSize int64
		// SweepMinThresholdSize defines the lower size of the sweep threshold. If sweeper will stop deleting
		// chunks from the local Storage if the size of the folder becomes less than this value.
		SweepMinThresholdSize int64
		// RemoteSyncThreshold defines the timeout between the last modification and now should be passed before
		// the chunk will be replicated remotely
		RemoteSyncThreshold time.Duration
		// SyncWorkers defines how many folders can be scanned and synced in parallel
		SyncWorkers int
		// GlobalSyncTimeout defines the timeout between scanning ALL chunk folders.
		GlobalSyncTimeout time.Duration
	}

	// Scanner structs represents a component which provides the file-system monitoring functionality and which
	// provides the local file-system cleans up and replication chunks to the remote Storage.
	Scanner struct {
		Replicator *Replicator `inject:""`

		logger logging.Logger
		cfg    ScannerConfig
	}

	scanInfos map[string]scanInfo

	scanInfo struct {
		ID       string     `json:"id"`
		Mtime    time.Time  `json:"mtime"`
		SyncTime *time.Time `json:"syncTime"`
		size     int64
	}
)

const (
	cScanFileName = "scan_info.json"
	cChunkInfoExt = ".info"
)

var _ linker.Initializer = (*Scanner)(nil)

// NewScanner creates the new instance of Scanner
func NewScanner(r *Replicator, cfg ScannerConfig) *Scanner {
	return &Scanner{logger: logging.NewLogger("chunkfs.Scanner"), cfg: cfg, Replicator: r}
}

// GetDefaultScannerConfig returns the default stand-alone Scanner config!
func GetDefaultScannerConfig() ScannerConfig {
	return ScannerConfig{
		DataPath:              "slog",
		SweepMaxThresholdSize: math.MaxInt64 / 2,
		SweepMinThresholdSize: math.MaxInt64 / 2,
		RemoteSyncThreshold:   time.Minute, // will sync after 1 minute of last modification time
		SyncWorkers:           0,           // no replication and scans at all
		GlobalSyncTimeout:     time.Minute,
	}
}

// String implements fmt.Stringer
func (sc ScannerConfig) String() string {
	b, _ := json.MarshalIndent(sc, "", "  ")
	return string(b)
}

// Init implements linker.Initializer
func (s *Scanner) Init(ctx context.Context) error {
	s.logger.Infof("initializing cfg:\n%s", s.cfg)
	go s.watcher(ctx)
	return nil
}

func (s *Scanner) watcher(ctx context.Context) {
	s.logger.Infof("starting watcher()")
	defer s.logger.Infof("exiting from watcher()")

	if s.cfg.SyncWorkers <= 0 {
		s.logger.Infof("the number of SyncWorkers in the config is zero or negative, exiting from the watcher")
		return
	}

	s.watcherWithF(ctx, s.scanDir)
}

func (s *Scanner) watcherWithF(ctx context.Context, f func(ctx context.Context, dir string)) {
	for {
		start := time.Now()
		fis := files.ListDir(s.cfg.DataPath)
		var wg sync.WaitGroup
		sema := make(chan struct{}, s.cfg.SyncWorkers)
		for _, fi := range fis {
			// ignore all not-dirs
			if !fi.IsDir() {
				continue
			}
			sema <- struct{}{}
			wg.Add(1)
			go func(dir string) {
				defer wg.Done()
				defer func() {
					<-sema
				}()
				f(ctx, dir)
			}(filepath.Join(s.cfg.DataPath, fi.Name()))
		}
		wg.Wait()
		diff := time.Now().Sub(start)
		if diff < s.cfg.GlobalSyncTimeout {
			select {
			case <-ctx.Done():
				// game over. Everything seems to be stopped
				return
			case <-time.After(s.cfg.GlobalSyncTimeout - diff):
				// ok, no leaks here, because it is the main case for the flow
			}
		}
	}
}

func newScanInfos() scanInfos {
	return make(scanInfos)
}

func (s *Scanner) scanDir(ctx context.Context, dir string) {
	sis := s.buildScanInfos(dir)

	sfn := filepath.Join(dir, cScanFileName)
	if err := sis.readFromFile(sfn); err != nil {
		s.logger.Warnf("could not read data from the file %s: %s", sfn, err)
	}

	s.sweep(ctx, dir, sis)
	s.replicate(ctx, sis)
	sis.writeToFile(sfn)
}

func (s *Scanner) buildScanInfos(dir string) scanInfos {
	sis := newScanInfos()
	for _, fi := range files.ListDir(dir) {
		if fi.IsDir() {
			continue
		}
		var si scanInfo
		if doesLookLikeID(fi.Name()) {
			si = scanInfo{ID: fi.Name(), Mtime: fi.ModTime(), size: fi.Size()}
		} else if isItScanInfoFile(fi.Name()) {
			var err error
			sifn := filepath.Join(dir, fi.Name())
			si, err = readScanInfo(sifn)
			_ = os.Remove(sifn)
			if err != nil {
				s.logger.Warnf("could not read scanInfo from %s: %s", sifn, err)
				continue
			}
		} else {
			continue
		}

		// merge the syncInfo (si) with what we have so far about the chunk ID
		if oldSI, ok := sis[si.ID]; ok {
			if oldSI.Mtime.After(si.Mtime) {
				si.Mtime = oldSI.Mtime
			}
			if si.SyncTime == nil {
				si.SyncTime = oldSI.SyncTime
			}
			si.size = max(si.size, oldSI.size)
		}
		sis[si.ID] = si
	}
	return sis
}

func (s *Scanner) sweep(ctx context.Context, dir string, sis scanInfos) {
	size := sis.size()
	if size < s.cfg.SweepMaxThresholdSize {
		// do nothing, we did not hit the local limit yet
		return
	}
	csis := container.Values(sis)
	s.logger.Infof("sweeping %s, current size=%d has %d chunks", dir, size, len(csis))
	sort.Slice(csis, func(i, j int) bool {
		return csis[i].Mtime.Before(csis[j].Mtime)
	})
	swept := 0
	for _, si := range csis {
		err := s.Replicator.DeleteChunk(ctx, si.ID, RFRemoteSync)
		if err != nil {
			s.logger.Debugf("could not sweep chunk %s: %s", si.ID, err)
			continue
		}
		size -= si.size
		swept++
		delete(sis, si.ID)
		if size < s.cfg.SweepMinThresholdSize {
			break
		}
	}
	s.logger.Infof("swept %d chunks in %s, the size now is %d ", swept, dir, size)
}

func (s *Scanner) replicate(ctx context.Context, sis scanInfos) {
	for _, si := range sis {
		now := time.Now()
		if now.Sub(si.Mtime) < s.cfg.RemoteSyncThreshold || (si.SyncTime != nil && si.SyncTime.After(si.Mtime)) {
			// no replication is needed
			continue
		}
		s.logger.Debugf("seems like the chunk %s should be replicated", si)
		if err := s.Replicator.UploadChunk(ctx, si.ID); err != nil {
			s.logger.Warnf("could not sync chunk %s: %s", si, err)
		} else {
			s.logger.Debugf("the chunk %s is replicated successfully", si)
			si.SyncTime = cast.Ptr(now)
			sis[si.ID] = si
		}
	}
}

func (sis scanInfos) size() int64 {
	res := int64(0)
	for _, si := range sis {
		res += si.size
	}
	return res
}

// readFromFile reads scanInfo objects complementary to the existing scanInfos objects
func (sis scanInfos) readFromFile(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		if errors.Is(err, errors.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	d := json.NewDecoder(f)
	infs := []scanInfo{}
	if err := d.Decode(&infs); err != nil {
		return err
	}

	for _, si := range infs {
		siExisting, ok := sis[si.ID]
		if !ok {
			continue
		}
		if siExisting.SyncTime == nil || (si.SyncTime != nil && si.SyncTime.After(*siExisting.SyncTime)) {
			siExisting.SyncTime = si.SyncTime
			sis[si.ID] = siExisting
		}
	}
	return nil
}

func (sis scanInfos) writeToFile(fn string) error {
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	e := json.NewEncoder(f)
	return e.Encode(container.Values(sis))
}

// String implements fmt.Srtinger
func (si scanInfo) String() string {
	return fmt.Sprintf("{ID:%s, Mtime:%s, SyncTime:%s, size:%d}", si.ID, si.Mtime, si.SyncTime, si.size)
}

// doesLookLikeID returns true if s looks like chunkID
func doesLookLikeID(s string) bool {
	return len(s) == ulid.EncodedSize
}

// isItScanInfoFile returns true if the fn is the scanInfo file name
func isItScanInfoFile(fn string) bool {
	return filepath.Ext(fn) == cChunkInfoExt && doesLookLikeID(fn[:len(fn)-len(cChunkInfoExt)])
}

// createScanInfo gets the filename fn and creates a new file with cChunkInfoExt extension for the file
func createScanInfo(cID, fn string) error {
	fi, err := os.Stat(fn)
	if err != nil {
		return err
	}
	return createScanInfoWithInfo(fn, scanInfo{ID: cID, SyncTime: cast.Ptr(time.Now()), Mtime: fi.ModTime()})
}

func createScanInfoWithInfo(fn string, si scanInfo) error {
	infoFN := fn + cChunkInfoExt
	infoFile, err := os.Create(infoFN)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.Remove(infoFN)
		}
	}()
	defer infoFile.Close()

	var info []byte
	info, err = json.Marshal(si)
	if err != nil {
		return err
	}
	_, err = infoFile.Write(info)
	return err
}

// readScanInfo tries to read the scanInfo json from the fn context
func readScanInfo(fn string) (scanInfo, error) {
	buf, err := os.ReadFile(fn)
	if err != nil {
		return scanInfo{}, err
	}
	var si scanInfo
	err = json.Unmarshal(buf, &si)
	return si, err
}
