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
	"github.com/siddontang/go-mysql/mysql"
)

type storage struct {
	db *sql.DB
}

func newStorage(db *sql.DB) *storage {
	return &storage{db}
}

func (s *storage) saveSnapShot(snapshot SnapShot, pos mysql.Position) error {
	return nil
}

func (s *storage) loadSnapShot(pos mysql.Position) (SnapShot, error) {
	return nil, nil
}
