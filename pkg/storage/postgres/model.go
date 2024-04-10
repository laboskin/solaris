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
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type (
	Log struct {
		ID        string    `db:"id"`
		Tags      Tags      `db:"tags"`
		Records   int64     `db:"records"`
		Deleted   bool      `db:"deleted"`
		CreatedAt time.Time `db:"created_at"`
		UpdatedAt time.Time `db:"updated_at"`
	}

	Tags map[string]string

	Chunk struct {
		ID           string `db:"id"`
		LogID        string `db:"log_id"`
		Min          string `db:"min"`
		Max          string `db:"max"`
		RecordsCount int    `db:"records"`
	}
)

func (t Tags) Value() (value driver.Value, err error) {
	return json.Marshal(t)
}

func (t *Tags) Scan(value any) error {
	buf, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("not a []byte value in scan")
	}
	return json.Unmarshal(buf, &t)
}

func (t Tags) JSON() string {
	var sb strings.Builder
	sb.WriteString("{")
	for k, v := range t {
		if sb.Len() > 1 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%q:%q", k, v))
	}
	sb.WriteString("}")
	return sb.String()
}
