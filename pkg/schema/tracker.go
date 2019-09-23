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
	"github.com/siddontang/go-mysql/mysql"

	tcontext "github.com/pingcap/dm/pkg/context"
)

// Tracker ...
type Tracker struct {
	curPos   mysql.Position
	executor *DDLExecutor
}

// NewTracker ...
func NewTracker() (*Tracker, error) {
	executor, err := NewDDLExecutor()
	if err != nil {
		// TODO change err to terror
		return nil, err
	}
	return &Tracker{
		executor: executor,
	}, nil

}

// GetTable ...
func (t *Tracker) GetTable(tctx *tcontext.Context, db string, table string) (*Table, error) {
	return t.executor.GetTable(tctx.Context(), db, table)
}

// Exec ...
func (t *Tracker) Exec(tctx *tcontext.Context, db string, sql string, pos mysql.Position) error {
	return t.executor.Execute(tctx.Context(), sql)
}
