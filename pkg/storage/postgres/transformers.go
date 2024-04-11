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
	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func logToModel(l *solaris.Log) Log {
	ml := Log{
		ID:   l.ID,
		Tags: l.Tags,
	}
	if l.CreatedAt != nil {
		ml.CreatedAt = l.CreatedAt.AsTime()
	}
	if l.UpdatedAt != nil {
		ml.UpdatedAt = l.UpdatedAt.AsTime()
	}
	return ml
}

func logToAPI(l Log) *solaris.Log {
	return &solaris.Log{
		ID:        l.ID,
		Tags:      l.Tags,
		CreatedAt: timestamppb.New(l.CreatedAt),
		UpdatedAt: timestamppb.New(l.UpdatedAt),
	}
}

func logsToAPI(ll []Log) []*solaris.Log {
	var all []*solaris.Log
	for _, l := range ll {
		all = append(all, logToAPI(l))
	}
	return all
}

func chunkToModel(c *logfs.ChunkInfo) Chunk {
	return Chunk{
		ID:           c.ID,
		Min:          c.Min.String(),
		Max:          c.Max.String(),
		RecordsCount: c.RecordsCount,
	}
}

func chunkToInfo(c Chunk) logfs.ChunkInfo {
	minVal, _ := ulid.Parse(c.Min)
	maxVal, _ := ulid.Parse(c.Max)
	return logfs.ChunkInfo{
		ID:           c.ID,
		Min:          minVal,
		Max:          maxVal,
		RecordsCount: c.RecordsCount,
	}
}

func chunksToInfo(cc []Chunk) []logfs.ChunkInfo {
	var cis []logfs.ChunkInfo
	for _, c := range cc {
		cis = append(cis, chunkToInfo(c))
	}
	return cis
}
