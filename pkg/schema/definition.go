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

import "strings"

// Column ...
type Column struct {
	Idx      int
	Name     string
	NotNull  bool
	Unsigned bool
	Tp       string
	Extra    string
}

// IsGeneratedColumn tells whether this column is generated
func (c *Column) IsGeneratedColumn() bool {
	return strings.Contains(c.Extra, "VIRTUAL GENERATED") || strings.Contains(c.Extra, "STORED GENERATED")
}

// Table ...
type Table struct {
	Schema string
	Name   string

	Columns      []*Column
	IndexColumns map[string][]*Column
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
