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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"time"
)

type (
	pgTestSuite struct {
		suite.Suite
		dbCont DbContainer
		db     *Db
	}
)

func (ts *pgTestSuite) SetupSuite() {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFn()
	dbCont, err := NewPgDbContainer(ctx, "postgres:16-alpine", WithDbName("solaris_test"))
	assert.Nil(ts.T(), err)

	// For non-container DB use NewNilDbContainer:
	//
	//dbCont, err := NewNilDbContainer(
	//	WithHost("127.0.0.1"),
	//	WithUser("postgres"),
	//	WithPassword("postgres"),
	//	WithPort("5432"),
	//	WithDbName("solaris_test"),
	//	WithSslMode("disable"))
	//assert.Nil(ts.T(), err)

	ts.dbCont = dbCont
}

func (ts *pgTestSuite) TearDownSuite() {
	if ts.db != nil {
		_ = ts.db.Close()
	}
	if ts.dbCont != nil {
		_ = ts.dbCont.Close()
	}
}

func (ts *pgTestSuite) BeforeTest(suiteName, testName string) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFn()

	dbCfg := ts.dbCont.DbConfig()
	assert.Nil(ts.T(), ts.dropCreatePgDb(ctx))

	var err error
	ts.db, err = GetDb(ctx, dbCfg.DataSourceFull())
	assert.Nil(ts.T(), err)
	assert.Nil(ts.T(), ts.db.Init(ctx))
}

func (ts *pgTestSuite) AfterTest(suiteName, testName string) {
	if ts.db != nil {
		ts.db.Shutdown()
		ts.db = nil
	}
}

func (ts *pgTestSuite) dropCreatePgDb(ctx context.Context) error {
	dbCfg := ts.dbCont.DbConfig()
	dbConn, err := sqlx.ConnectContext(ctx, "postgres", dbCfg.DataSourceNoDb())
	if err != nil {
		return err
	}
	defer func() {
		_ = dbConn.Close()
	}()
	if _, err = dbConn.DB.Exec(fmt.Sprintf("drop database if exists %s with (force)", dbCfg.DbName)); err != nil {
		return err
	}
	if _, err = dbConn.DB.Exec(fmt.Sprintf("create database %s", dbCfg.DbName)); err != nil {
		return err
	}
	return nil
}
