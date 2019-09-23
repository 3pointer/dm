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

package mocktidb

import (
	"context"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

type DDLExecutor struct {
	session  session.Session
}


// NewDDLExecutor represents a mock tidb session use memory to store data
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
		session:  session,
	}, nil
}

// Execute executes the DDL
func (e *DDLExecutor) Execute(context context.Context, sql string) error {
	_, err := e.session.Execute(context, sql)
	if err != nil {
		return err
	}
	return nil
}

func (e *DDLExecutor) GetTable(context context.Context, schema string, table string) *table {

}

// Query executes sql return Rows
//func (e *DDLExecutor) Query(context context.Context, sql string) error {
//	rows, err := e.session.Execute(context, sql)
//	if err != nil {
//		return err
//	}
//	for _, row := range rows {
//		res, err := session.ResultSetToStringSlice(context, e.session, row)
//		if err != nil {
//			fmt.Println(res)
//		}
//	}
//	return nil
//}
