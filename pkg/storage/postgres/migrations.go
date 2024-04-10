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
	"database/sql"
	migrate "github.com/rubenv/sql-migrate"
)

const (
	initSchemaUp = `
create table if not exists "log"
(
    "id"          varchar(32)              not null,
    "tags"        jsonb                    not null default '{}'::jsonb,
    "records"     integer                  not null default 0,
    "deleted"     boolean                  not null default false,
    "created_at"  timestamp with time zone not null default (now() at time zone 'utc'),
    "updated_at"  timestamp with time zone not null default (now() at time zone 'utc'),
    primary key ("id")
);

create index if not exists "idx_log_tags" on "log" using gin ("tags");

create table if not exists "chunk"
(
    "id"          varchar(32)              not null,
    "log_id"      varchar(32) references "log" ("id") on delete cascade,
    "min"         varchar(32)              not null default '',
    "max"         varchar(32)              not null default '',
    "records"     integer                  not null default 0,
    primary key ("log_id", "id")
);
`
	initSchemaDown = `
drop table if exists "log";
drop table if exists "chunk";
`
)

func initSchema(id string) *migrate.Migration {
	return &migrate.Migration{
		Id:   id,
		Up:   []string{initSchemaUp},
		Down: []string{initSchemaDown},
	}
}

func migrations() []*migrate.Migration {
	return []*migrate.Migration{
		initSchema("0"),
	}
}

func migrateUp(ctx context.Context, db *sql.DB) error {
	mms := migrate.MemoryMigrationSource{Migrations: migrations()}
	if _, err := migrate.ExecContext(ctx, db, "postgres", mms, migrate.Up); err != nil {
		return err
	}
	return nil
}

func migrateDown(ctx context.Context, db *sql.DB) error {
	mms := migrate.MemoryMigrationSource{Migrations: migrations()}
	if _, err := migrate.ExecContext(ctx, db, "postgres", mms, migrate.Down); err != nil {
		return err
	}
	return nil
}
