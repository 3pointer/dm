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
	"fmt"
	"github.com/pingcap/dm/pkg/terror"
	"strings"
)

// Database ...
type Database map[string]*Table

// Column ...
type Column struct {
	Idx      int    `json:"idx"`
	Name     string `json:"name"`
	NotNull  bool   `json:"not_null"`
	Unsigned bool   `json:"unsigned"`
	Tp       string `json:"tp"`
	Extra    string `json:"extra"`
}

// IsGeneratedColumn tells whether this column is generated
func (c *Column) IsGeneratedColumn() bool {
	return strings.Contains(c.Extra, "VIRTUAL GENERATED") || strings.Contains(c.Extra, "STORED GENERATED")
}

// Table ...
type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`

	Columns      []*Column            `json:"columns"`
	IndexColumns map[string][]*Column `json:"index_columns"`
}

func findColumn(columns []*Column, indexColumn string) *Column {
	for _, column := range columns {
		if column.Name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*Column, indexColumns map[string][]string) map[string][]*Column {
	result := make(map[string][]*Column)

	for keyName, indexCols := range indexColumns {
		cols := make([]*Column, 0, len(indexCols))
		for _, name := range indexCols {
			column := findColumn(columns, name)
			if column != nil {
				cols = append(cols, column)
			}
		}
		result[keyName] = cols
	}

	return result
}

func getTableIndex(db *sql.DB, table *Table) error {
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.Schema, table.Name)
	rows, err := db.Query(query)
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

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
	var columns = make(map[string][]string)
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := strings.ToLower(string(data[2]))
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	table.IndexColumns = findColumns(table.Columns, columns)
	return nil
}

func getTableColumns(db *sql.DB, tableName string) (*Table, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

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

	idx := 0
	table := &Table{}
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		column := &Column{}
		column.Idx = idx
		column.Name = string(data[0])
		column.Tp = string(data[1])
		column.Extra = string(data[5])

		if strings.ToLower(string(data[2])) == "no" {
			column.NotNull = true
		}

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.Unsigned = true
		}

		table.Columns = append(table.Columns, column)
		idx++
	}

	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	return table, nil
}
