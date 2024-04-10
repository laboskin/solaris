package chunkfs

import (
	"context"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/sss/inmem"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestScanner_watcher(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestScanner_watcher")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	s := testNewScanner(dir, GetDefaultScannerConfig())
	// must return immediately, so no workers
	s.watcher(context.Background())

	s.cfg.SyncWorkers = 1
	s.cfg.GlobalSyncTimeout = 10 * time.Millisecond
	s.cfg.DataPath = dir

	var lock sync.Mutex
	m := make(map[string]int)
	scanF := func(_ context.Context, dir string) {
		lock.Lock()
		defer lock.Unlock()
		m[dir]++
	}
	d1 := filepath.Join(dir, "dir1")
	d2 := filepath.Join(dir, "dir2")
	files.EnsureDirExists(d1)
	files.EnsureDirExists(d2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	s.watcherWithF(ctx, scanF)
	assert.NotNil(t, ctx.Err())
	assert.Equal(t, 2, len(m))
	assert.True(t, m[d1] >= 1)
	assert.True(t, m[d2] >= 1)
	assert.Equal(t, m[d1], m[d2])
}

func TestScanner_buildScanInfos(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestScanner_buildScanInfos")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	s := testNewScanner(dir, GetDefaultScannerConfig())
	c1, c2 := ulidutils.NewID(), ulidutils.NewID()

	files.EnsureDirExists(filepath.Join(dir, ulidutils.NewID()))
	createRandomFile(t, filepath.Join(dir, c1))
	createRandomFile(t, filepath.Join(dir, c2))
	tt := time.Now()
	createScanInfoWithInfo(filepath.Join(dir, c2), scanInfo{ID: c2, SyncTime: cast.Ptr(tt), Mtime: tt, size: 1000})
	sfi := filepath.Join(dir, c2+cChunkInfoExt)
	_, err = os.Stat(sfi)
	assert.Nil(t, err)
	sis := s.buildScanInfos(dir)
	assert.Equal(t, 2, len(sis))
	assert.Nil(t, sis[c1].SyncTime)
	assert.Equal(t, int64(512), sis[c1].size)

	assert.Equal(t, sis[c2].SyncTime.UnixMilli(), tt.UnixMilli())
	assert.Equal(t, sis[c2].Mtime.UnixMilli(), tt.UnixMilli())
	assert.Equal(t, int64(512), sis[c2].size)

	_, err = os.Stat(sfi)
	assert.True(t, errors.Is(err, errors.ErrNotExist))
}

func TestScanner_sweep(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestScanner_sweep")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	s := testNewScanner(dir, ScannerConfig{SweepMaxThresholdSize: 1000, SweepMinThresholdSize: 600})
	sis := scanInfos{"100": {ID: "100", size: 1}, "200": {ID: "200", size: 2}}
	s.sweep(context.Background(), dir, sis)
	assert.Equal(t, 2, len(sis))

	sis = scanInfos{
		"100": {ID: "100", size: 580, Mtime: time.Now()},
		"200": {ID: "200", size: 200, Mtime: time.Now().Add(-time.Minute)},
		"300": {ID: "300", size: 500, Mtime: time.Now().Add(-time.Minute)},
		"400": {ID: "400", size: 19, Mtime: time.Now().Add(-2 * time.Minute)},
	}
	s.sweep(context.Background(), dir, sis)
	// no files, so the upload should be skipped
	assert.Equal(t, 4, len(sis))

	createRandomFile(t, filepath.Join(dir, "100"))
	createRandomFile(t, filepath.Join(dir, "200"))
	createRandomFile(t, filepath.Join(dir, "300"))

	s.sweep(context.Background(), dir, sis)
	// no files, so the upload should be skipped
	assert.Equal(t, 2, len(sis))
	lst, err := s.Replicator.Storage.List(context.Background(), "/00/")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(lst))
}

func TestScanner_replicate(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestScanner_replicate")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	s := testNewScanner(dir, GetDefaultScannerConfig())
	sis := newScanInfos()
	sis["100"] = scanInfo{ID: "100", Mtime: time.Now()}
	sis["210"] = scanInfo{ID: "210", SyncTime: cast.Ptr(time.Now())}
	s.replicate(context.Background(), sis)
	lst, err := s.Replicator.Storage.List(context.Background(), "/")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(lst))

	// error when upload
	sis["100"] = scanInfo{ID: "100", Mtime: time.Now().Add(-time.Hour)}
	s.replicate(context.Background(), sis)
	lst, err = s.Replicator.Storage.List(context.Background(), "/")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(lst))
	assert.Nil(t, sis["100"].SyncTime)

	// create both file, so it should be uploaded ok
	createRandomFile(t, filepath.Join(dir, "100"))
	createRandomFile(t, filepath.Join(dir, "210"))
	sis["210"] = scanInfo{ID: "210", Mtime: time.Now().Add(-time.Hour / 2), SyncTime: cast.Ptr(time.Now().Add(-time.Hour))}
	before := time.Now()
	s.replicate(context.Background(), sis)
	assert.True(t, sis["100"].SyncTime.After(before))
	assert.True(t, sis["210"].SyncTime.After(before))
	lst, err = s.Replicator.Storage.List(context.Background(), "/")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(lst))
	lst, err = s.Replicator.Storage.List(context.Background(), "/00/")
	assert.Nil(t, err)
	assert.Equal(t, "/00/100", lst[0])
}

func TestScanInfos_size(t *testing.T) {
	sis := newScanInfos()
	sis["lala"] = scanInfo{ID: "lala", Mtime: time.Now(), SyncTime: cast.Ptr(time.Now().Add(time.Second)), size: 123}
	sis["lala1"] = scanInfo{ID: "lala1", Mtime: time.Now(), SyncTime: cast.Ptr(time.Now().Add(time.Second)), size: 0}
	sis["lala2"] = scanInfo{ID: "lala2", Mtime: time.Now(), SyncTime: cast.Ptr(time.Now().Add(time.Second)), size: 77}
	assert.Equal(t, int64(200), sis.size())
}

func TestScanInfos_readWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestScanInfos_readWrite")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	sis := newScanInfos()
	sis["lala"] = scanInfo{ID: "lala", Mtime: time.Now(), SyncTime: cast.Ptr(time.Now().Add(time.Second))}
	sis["lala2"] = scanInfo{ID: "lala2", Mtime: time.Now(), SyncTime: cast.Ptr(time.Now().Add(time.Second))}

	fn := filepath.Join(dir, "test.json")
	assert.Nil(t, sis.writeToFile(fn))
	sis1 := newScanInfos()
	assert.Nil(t, sis1.readFromFile(fn))
	assert.Equal(t, 0, len(sis1))

	si := sis["lala"]
	si.SyncTime = nil
	sis1["lala"] = si
	assert.NotEqual(t, sis1["lala"], sis["lala"])
	assert.Nil(t, sis1.readFromFile(fn))
	assert.Equal(t, 1, len(sis1))
	assert.Equal(t, sis1["lala"].SyncTime.UnixMilli(), sis["lala"].SyncTime.UnixMilli())
}

func Test_doesLookLikeID(t *testing.T) {
	s := ulidutils.NewID()
	assert.False(t, isItScanInfoFile(s+"dd"))
	assert.False(t, isItScanInfoFile(s[:len(s)-1]))
	assert.True(t, doesLookLikeID(s))
}

func Test_isItScanInfoFile(t *testing.T) {
	s := ulidutils.NewID()
	assert.False(t, isItScanInfoFile(s))
	assert.True(t, isItScanInfoFile(s+cChunkInfoExt))
	assert.False(t, isItScanInfoFile("lalal"+cChunkInfoExt))
}

func Test_CreateAndReadScanInfo(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestReplicator_SimpleDelete")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cID := "lala"
	fn := filepath.Join(dir, cID)
	sifn := fn + cChunkInfoExt

	_, err = readScanInfo(sifn)
	assert.NotNil(t, err)
	err = createScanInfo(cID, fn)
	assert.NotNil(t, err)

	createRandomFile(t, fn)
	_, err = readScanInfo(sifn)
	assert.NotNil(t, err)
	err = createScanInfo(cID, fn)
	assert.Nil(t, err)
	si, err := readScanInfo(sifn)
	assert.Nil(t, err)
	fi, _ := os.Stat(fn)
	assert.True(t, time.Now().After(*si.SyncTime) || time.Now().Equal(*si.SyncTime))
	assert.Equal(t, fi.ModTime().UTC(), si.Mtime.UTC())
	assert.Equal(t, si.ID, cID)
}

func testNewScanner(dir string, cfg ScannerConfig) *Scanner {
	r := NewReplicator(func(v string) string {
		return filepath.Join(dir, v)
	})
	r.Storage = inmem.NewStorage()
	r.CA = NewChunkAccessor()
	s := NewScanner(r, cfg)
	return s
}
