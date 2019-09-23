// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"database/sql"

	"github.com/pingcap/tidb-tools/pkg/filter"
	route "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/utils"
)

// Tracker ...
type Tracker struct {
	curPos   mysql.Position
	executor *DDLExecutor

	storage *storage
}

// NewTracker ...
// TODO change err to terror
func NewTracker(
	pos mysql.Position, fromDB *sql.DB, toDB *sql.DB,
	bwList *filter.Filter, tableRouter *route.Table) (*Tracker, error) {

	storage := newStorage()
	currentPosSnapShot, err := storage.loadSnapShot(pos)
	if err != nil {
		return nil, err
	}

	DBSnapShot, err := fetchSnapShotFromDB(fromDB, toDB, bwList, tableRouter)
	if err != nil {
		return nil, err
	}

	// two snapshot may have different keys(schema, table)
	// this could happen when user 1. stop task 2. update bwList 3. resume task
	// so we should merge DBSnapShot into currentPosSnapShot
	// if both have same keys, keep currentPosSnapShot values
	// if both have different keys, use DBSnapShot values
	snapshot := mergeSnapShot(currentPosSnapShot, DBSnapShot)

	executor, err := NewDDLExecutor()
	if err != nil {
		return nil, err
	}

	err = executor.restore(snapshot)
	if err != nil {
		return nil, err
	}

	return &Tracker{
		executor: executor,
		storage:  storage,
	}, nil

}

func mergeSnapShot(currentPosSnapShot, DBSnapShot map[string]Database) map[string]Database {
	for dbName, tables := range currentPosSnapShot {
		if _, ok := DBSnapShot[dbName]; !ok {
			delete(currentPosSnapShot, dbName)
		} else {
			DBTables := DBSnapShot[dbName]
			for tableName := range tables {
				if _, ok := DBTables[tableName]; !ok {
					delete(currentPosSnapShot[dbName], tableName)
				}
			}
		}
	}

	for dbName, tables := range DBSnapShot {
		if _, ok := currentPosSnapShot[dbName]; !ok {
			currentPosSnapShot[dbName] = tables
		} else {
			posTables := currentPosSnapShot[dbName]
			for tableName, table := range tables {
				if _, ok := posTables[tableName]; !ok {
					posTables[tableName] = table
				}
			}
		}
	}

	return currentPosSnapShot
}

func fetchSnapShotFromDB(fromDB *sql.DB, toDB *sql.DB, bwList *filter.Filter, router *route.Table) (map[string]Database, error) {
	targetTablesMap, err := utils.FetchTargetDoTables(fromDB, bwList, router)
	if err != nil {
		return nil, err
	}
	dbs := make(map[string]Database)
	for targetTable, sourceTables := range targetTablesMap {
		// use downstream table schema as upstream table schema
		// because in incremental mode, upstream schema may different from current checkpoint's schema
		// but downstream schema is same as current checkpoint's schema
		table, err := getTableColumns(toDB, targetTable)
		if err != nil {
			return nil, err
		}
		err = getTableIndex(toDB, table)
		if err != nil {
			return nil, err
		}
		for _, sourceTable := range sourceTables {
			dbs[sourceTable.Schema][sourceTable.Name] = table
		}
	}

	return dbs, nil
}

// GetTable ...
func (t *Tracker) GetTable(tctx *tcontext.Context, db string, table string) (*Table, error) {
	return t.executor.GetTable(tctx.Context(), db, table)
}

// Exec ...
func (t *Tracker) Exec(tctx *tcontext.Context, db string, sql string, pos mysql.Position) error {
	return t.executor.Execute(tctx.Context(), sql)
}
