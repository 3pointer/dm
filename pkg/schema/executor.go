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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

// DDLExecutor ...
type DDLExecutor struct {
	session session.Session

	databases map[string]Database
}

// NewDDLExecutor represents a mock tidb session use memory to store data
// TODO change err to terror
func NewDDLExecutor() (*DDLExecutor, error) {
	mockTikv, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, err
	}
	_, err = session.BootstrapSession(mockTikv)
	if err != nil {
		return nil, err
	}
	session, err := session.CreateSession4Test(mockTikv)
	if err != nil {
		return nil, err
	}
	return &DDLExecutor{
		session: session,
	}, nil
}

// Execute executes the DDL
func (e *DDLExecutor) Execute(ctx context.Context, sql string) error {
	_, err := e.session.Execute(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

// GetTable ...
func (e *DDLExecutor) GetTable(ctx context.Context, schema string, name string) (*Table, error) {
	table := &Table{}
	table.Schema = schema
	table.Name = name

	// get columns
	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------------------+
	   | Field | Type    | Null | Key | Default | Extra             |
	   +-------+---------+------+-----+---------+-------------------+
	   | a     | int(11) | NO   | PRI | NULL    |                   |
	   | b     | int(11) | NO   | PRI | NULL    |                   |
	   | c     | int(11) | YES  | MUL | NULL    |                   |
	   | d     | int(11) | YES  |     | NULL    |                   |
	   | d     | json    | YES  |     | NULL    | VIRTUAL GENERATED |
	   +-------+---------+------+-----+---------+-------------------+
	*/
	sql := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", schema, name)
	recordSets, err := e.session.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	for _, row := range recordSets {
		chunkRows, err := session.GetRows4Test(ctx, e.session, row)
		if err != nil {
			return nil, err
		}
		for idx, chunkRow := range chunkRows {
			column := &Column{}
			column.Idx = idx
			column.Name = chunkRow.GetString(0)
			column.Tp = chunkRow.GetString(1)
			column.Extra = chunkRow.GetString(5)

			if strings.ToLower(chunkRow.GetString(2)) == "no" {
				column.NotNull = true
			}

			// Check whether Column has unsigned flag.
			if strings.Contains(strings.ToLower(column.Tp), "unsigned") {
				column.Unsigned = true
			}
			table.Columns = append(table.Columns, column)
		}
	}

	// get index
	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	sql = fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", schema, name)
	recordSets, err = e.session.Execute(ctx, sql)
	columns := make(map[string][]string)

	for _, row := range recordSets {
		chunkRows, err := session.GetRows4Test(ctx, e.session, row)
		if err != nil {
			return nil, err
		}
		for _, chunkRow := range chunkRows {

			nonUnique := chunkRow.GetInt64(1)
			if nonUnique == 0 {
				keyName := strings.ToLower(chunkRow.GetString(2))
				columns[keyName] = append(columns[keyName], chunkRow.GetString(4))
			}
		}
	}
	table.IndexColumns = findColumns(table.Columns, columns)

	return nil, nil
}

func (e *DDLExecutor) restore(snapshot map[string]Database) error {
	e.databases = snapshot
	return nil
}
