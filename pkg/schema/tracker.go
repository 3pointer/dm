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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/tidb-tools/pkg/filter"
	route "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/utils"
)

// Tracker ...
type Tracker struct {
	executor *DDLExecutor

	storage *storage
}

// NewTracker ...
// TODO change err to terror
func NewTracker(
	pos mysql.Position, fromDB *sql.DB, toDB *sql.DB,
	bwList *filter.Filter, tableRouter *route.Table) (*Tracker, error) {

	storage := newStorage(toDB)
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

	ctx := context.Background()
	err = executor.restore(ctx, snapshot)
	if err != nil {
		return nil, err
	}

	return &Tracker{
		executor: executor,
		storage:  storage,
	}, nil

}

func mergeSnapShot(currentPosSnapShot, DBSnapShot SnapShot) SnapShot {
	for dbName, db := range currentPosSnapShot {
		if _, ok := DBSnapShot[dbName]; !ok {
			delete(currentPosSnapShot, dbName)
		} else {
			DBTables := DBSnapShot[dbName].Tables
			for tableName := range db.Tables {
				if _, ok := DBTables[tableName]; !ok {
					delete(currentPosSnapShot[dbName].Tables, tableName)
				}
			}
		}
	}

	for dbName, db := range DBSnapShot {
		if _, ok := currentPosSnapShot[dbName]; !ok {
			currentPosSnapShot[dbName] = db
		} else {
			posTables := currentPosSnapShot[dbName].Tables
			for tableName, table := range db.Tables {
				if _, ok := posTables[tableName]; !ok {
					posTables[tableName] = table
				}
			}
		}
	}

	return currentPosSnapShot
}

func fetchSnapShotFromDB(fromDB *sql.DB, toDB *sql.DB, bwList *filter.Filter, router *route.Table) (SnapShot, error) {
	targetTablesMap, err := utils.FetchTargetDoTables(fromDB, bwList, router)
	if err != nil {
		return nil, err
	}
	snapshot := make(SnapShot)
	for targetTableSchema, sourceTables := range targetTablesMap {
		keys := strings.Split(targetTableSchema, ".")
		if len(keys) != 2 {
			return nil, errors.New("targetTablesMap key errror")
		}
		targetSchema := keys[0]
		targetTable := keys[1]

		dbQuery := fmt.Sprintf(showCreateDBSQL, targetSchema)
		createDBSQL, err := getCreateSQL(toDB, dbQuery)
		if err != nil {
			return nil, err
		}
		tableQuery := fmt.Sprintf(showCreateTableSQL, targetSchema, targetTable)
		createTableSQL, err := getCreateSQL(toDB, tableQuery)
		if err != nil {
			return nil, err
		}

		// use downstream table schema as upstream table schema
		// because in incremental mode, upstream schema may different from current checkpoint's schema
		// but downstream schema is same as current checkpoint's schema
		for _, sourceTable := range sourceTables {
			createDBSQL = strings.Replace(createDBSQL, fmt.Sprintf("DATABASE `%s`", targetSchema),
				fmt.Sprintf("DATABASE `%s`", sourceTable.Schema), -1)

			createTableSQL = strings.Replace(createTableSQL, fmt.Sprintf("TABLE `%s`", targetTable),
				fmt.Sprintf("TABLE `%s`", sourceTable.Name), -1)

			if _, ok := snapshot[sourceTable.Schema]; !ok {
				snapshot[sourceTable.Schema] = Database{
					CreateDBSQL: createDBSQL,
					Tables:      make(map[string]string),
				}
			}
			snapshot[sourceTable.Schema].Tables[sourceTable.Name] = createTableSQL
		}
	}

	return snapshot, nil
}

// GetTable ...
func (t *Tracker) GetTable(tctx *tcontext.Context, db string, table string) (*Table, error) {
	return t.executor.getTable(tctx.Context(), db, table)
}

// Exec ...
func (t *Tracker) Exec(tctx *tcontext.Context, db string, sql string, pos mysql.Position) error {
	err := t.executor.execute(tctx.Context(), db, sql)
	if err != nil {
		return err
	}
	snapshot, err := t.executor.snapshot(tctx.Context())
	err = t.storage.saveSnapShot(snapshot, pos)
	if err != nil {
		return err
	}
	return nil
}
