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
	"fmt"
	"github.com/logrange/linker"
	"github.com/solarisdb/solaris/golibs/errors"
	"sync"
)

type (
	// ChunkAccessor implements FSM for sharing access to the local chunk files. It keeps the states for every chunk file, and
	// it serves as a synchronization barrier between Chunk and Replicator objects, that may touch the chunk
	// files in parallel.
	ChunkAccessor struct {
		lock   sync.Mutex
		chunks map[string]*caRec
		closed bool
		doneCh chan struct{}
	}

	caRec struct {
		state    int
		opened   bool
		waiterCh chan struct{}
	}
)

const (
	cStateUnknown  = 0
	cStateIdle     = 1
	cStateWriting  = 2
	cStateDeleting = 3
)

// NewChunkAccessor creates the new ChunkAccessor
func NewChunkAccessor() *ChunkAccessor {
	return &ChunkAccessor{chunks: make(map[string]*caRec), doneCh: make(chan struct{})}
}

var _ linker.Shutdowner = (*ChunkAccessor)(nil)

// Shutdown - closes the ChunkAccessor
func (cc *ChunkAccessor) Shutdown() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if cc.closed {
		return
	}
	cc.closed = true
	close(cc.doneCh)
}

func (cc *ChunkAccessor) openChunk(ctx context.Context, cID string) error {
	for {
		cc.lock.Lock()
		if cc.closed {
			cc.lock.Unlock()
			return errors.ErrClosed
		}
		cr, ok := cc.chunks[cID]
		if !ok {
			cr = &caRec{state: cStateIdle}
			cc.chunks[cID] = cr
		}
		if cr.state == cStateDeleting {
			cc.lock.Unlock()
			return fmt.Errorf("raise: open while deleting %v: %w", cr, errors.ErrNotExist)
		}
		if cr.opened {
			cc.lock.Unlock()
			return fmt.Errorf("unaccepted state in openChunk() %v: %w", cr, errors.ErrInternal)
		}
		cr.opened = true
		if cr.state == cStateIdle {
			// it seems just created, we are fine then
			cc.lock.Unlock()
			return nil
		}
		ch := cr.getWaiterCh()
		cc.lock.Unlock()

		select {
		case <-ctx.Done():
			cc.closeChunk(cID)
			return ctx.Err()
		case <-cc.doneCh:
			cc.closeChunk(cID)
			return errors.ErrClosed
		case <-ch:
			// ok, let's try again
			cc.closeChunk(cID)
		}
	}
}

// SetWriting requests writing access to the chunk. The function must followed by SetIdle() call to release the write access
func (cc *ChunkAccessor) SetWriting(ctx context.Context, cID string) error {
	for {
		cc.lock.Lock()
		if cc.closed {
			cc.lock.Unlock()
			return errors.ErrClosed
		}
		cr, ok := cc.chunks[cID]
		if !ok {
			cr = &caRec{state: cStateWriting}
			cc.chunks[cID] = cr
			cc.lock.Unlock()
			return nil
		}
		if cr.state == cStateIdle {
			cr.setState(cStateWriting)
			cc.lock.Unlock()
			return nil
		}
		if cr.state == cStateDeleting {
			cc.lock.Unlock()
			return fmt.Errorf("the chunk file is being deleted: %w", errors.ErrNotExist)
		}
		ch := cr.getWaiterCh()
		cc.lock.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cc.doneCh:
			return errors.ErrClosed
		case <-ch:
			// ok, let's try again
		}
	}
}

// setDeleting tries to set Deleting state, and it returns true if successlul. It will return false otherwise.
func (cc *ChunkAccessor) setDeleting(cID string) bool {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if cc.closed {
		return false
	}
	cr, ok := cc.chunks[cID]
	if !ok {
		cr = &caRec{state: cStateDeleting}
		cc.chunks[cID] = cr
	}
	return !ok
}

// SetIdle closes the Writing (SetWriting) and Deleting (SetDeleting) exclusive access
func (cc *ChunkAccessor) SetIdle(cID string) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cr, ok := cc.chunks[cID]
	if !ok {
		return
	}
	cr.setState(cStateIdle)
	if !cr.opened {
		delete(cc.chunks, cID)
	}
}

func (cc *ChunkAccessor) closeChunk(cID string) error {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cr, ok := cc.chunks[cID]
	if !ok || !cr.opened {
		return errors.ErrClosed
	}
	cr.opened = false
	if cr.state == cStateIdle {
		cr.setState(cStateUnknown) // do this to notify waiters if any
		delete(cc.chunks, cID)
	}
	return nil
}

func (cr *caRec) setState(newState int) {
	if cr.state == newState {
		return
	}
	cr.state = newState
	if cr.waiterCh != nil {
		close(cr.waiterCh)
		cr.waiterCh = nil
	}
}

func (cr *caRec) getWaiterCh() chan struct{} {
	if cr.waiterCh == nil {
		cr.waiterCh = make(chan struct{})
	}
	return cr.waiterCh
}
