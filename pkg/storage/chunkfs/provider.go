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
	"github.com/logrange/linker"
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Provider manages a pull of opened chunks and allows to return a Chunk object by request.
// The Provider limits the number of opened file descriptors and the space on the local drive
// borrowed for the chunks
type Provider struct {
	Replicator *Replicator    `inject:""`
	CA         *ChunkAccessor `inject:""`

	logger logging.Logger
	dir    string
	ccfg   Config
	closed atomic.Bool
	chunks *lru.ReleasableCache[string, *Chunk]
}

// NewProvider creates the new Provider instance
func NewProvider(dir string, maxOpenedChunks int, cfg Config) *Provider {
	p := new(Provider)
	p.logger = logging.NewLogger("chunkfs.Provider")
	p.dir = dir
	p.ccfg = cfg
	var err error
	p.chunks, err = lru.NewReleasableCache[string, *Chunk](maxOpenedChunks, p.openChunk, p.closeChunk)
	if err != nil {
		panic(err)
	}
	return p
}

var _ linker.Shutdowner = (*Provider)(nil)

// Shutdown implements linker.Shutdowner
func (p *Provider) Shutdown() {
	p.Close()
}

// GetOpenedChunk returns a lru.Releasable object for the *Chunk (ready to be used) by its ID.
// The function may return ctx.Err() or ErrClosed errors
func (p *Provider) GetOpenedChunk(ctx context.Context, cID string, newFile bool) (lru.Releasable[*Chunk], error) {
	if newFile && !p.closed.Load() {
		if err := files.EnsureFileExists(p.GetFileNameByID(cID)); err != nil {
			return lru.Releasable[*Chunk]{}, err
		}
	}
	return p.chunks.GetOrCreate(ctx, cID)
}

// DeleteFileIfEmpty deletes the file chunk if it is empty
func (p *Provider) DeleteFileIfEmpty(cID string) {
	if len(cID) == 0 {
		return
	}
	if !p.CA.setDeleting(cID) {
		p.logger.Errorf("DeleteFileIfEmpty(): something goes wrong, cannot mark chunk %s for deleting", cID)
	}
	defer p.CA.SetIdle(cID)
	fn := p.GetFileNameByID(cID)
	fi, err := os.Stat(fn)
	if err == nil && fi.Size() == 0 {
		os.Remove(fn)
	}
}

// Close implements the io.Closer
func (p *Provider) Close() error {
	p.closed.Store(true)
	p.logger.Infof("Close() called")
	return p.chunks.Close()
}

// ReleaseChunk must be called as soon as the chunk is not needed anymore
func (p *Provider) ReleaseChunk(r *lru.Releasable[*Chunk]) {
	p.chunks.Release(r)
}

// GetFileNameByID returns the filename for the chunk ID cID provided
func (p *Provider) GetFileNameByID(cID string) string {
	return filepath.Join(p.getPathByID(cID), cID)
}

func (p *Provider) openChunk(ctx context.Context, cID string) (*Chunk, error) {
	if err := p.CA.openChunk(ctx, cID); err != nil {
		return nil, err
	}
	fn := p.GetFileNameByID(cID)
	downloaded, err := p.downloadFileIfNotExists(ctx, cID, fn)
	if err != nil {
		p.logger.Warnf("could not download chunk cID=%s from the remote storage: %s", cID, err)
		p.CA.closeChunk(cID)
		return nil, err
	}

	c := NewChunk(p.GetFileNameByID(cID), cID, p.ccfg)
	p.logger.Debugf("opening chunk %v", c)
	err = c.Open(false)
	if errors.Is(err, errCorrupted) && !downloaded {
		p.logger.Warnf("tried to open the chunk=%v, but got the corrupted error, will sync the file from remote and try again: %v", c, err)
		_ = os.Remove(c.fn)
		_, err = p.downloadFileIfNotExists(ctx, cID, fn)
		if err == nil {
			err = c.Open(false)
		}
	}

	if err != nil {
		p.logger.Errorf("could not open the chunk=%v. Unrecoverable error, will give up with the chunk for awhile: %v", c, err)
		p.CA.closeChunk(cID)
	} else {
		p.logger.Infof("the chunk=%v is opened ok", c)
	}

	return c, err
}

func (p *Provider) downloadFileIfNotExists(ctx context.Context, cID, fn string) (bool, error) {
	if _, err := os.Stat(fn); err != nil {
		p.logger.Infof("downloadFileIfNotExists - no file for cID=%s on the local file system: %s", cID, err)
		if err := p.Replicator.DownloadChunk(ctx, cID, RFRemoteSync); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (p *Provider) closeChunk(cID string, c *Chunk) {
	if err := p.CA.closeChunk(cID); err != nil {
		p.logger.Errorf("something goes wrong. closeChunk(%s) reported the error: %s", cID, err)
	}
	if err := c.Close(); err != nil {
		p.logger.Warnf("could not close chunk c=%v", c)
	}
}

func (p *Provider) getPathByID(id string) string {
	ln := len(id)
	return filepath.Join(p.dir, id[ln-2:ln])
}
