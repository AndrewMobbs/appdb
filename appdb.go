/*
Copyright © 2021 Andrew Mobbs <andrew.mobbs@gmail.com>

This program is free software; you can redistribute it and/or
modify it under the terms of version 2 of the GNU General Public
License as published by the Free Software Foundation;

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; If not, see <http://www.gnu.org/licenses/>.
*/
package appdb

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3" // Import go-sqlite3 library
)

type SchemaVersionError struct {
	Version         uint8
	ExpectedVersion uint8
}

func (e *SchemaVersionError) Error() string {
	return fmt.Sprintf("Incorrect Schema Version: Got %d - Expected %d", e.Version, e.ExpectedVersion)
}

type AppIdError struct {
	Id         uint32
	ExpectedId uint32
}

func (e *AppIdError) Error() string {
	return fmt.Sprintf("Incorrect Database App Id: Got %d - Expected %d", e.Id, e.ExpectedId)
}

type SchemaError struct {
	Statement string
	Err       error
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("Error %s creating schema on statement %s", e.Err, e.Statement)
}

// InitSqlLiteDB initialises a sqlite3 database at the given path, opening if it exists, creating file & path if not
func InitAppDB(dbPath string, appName string, schemaVersion uint8, schema []string) (*sql.DB, error) {
	log.Println("InitAppDb(", dbPath, appName, schemaVersion, ")")
	_, err := os.Stat(dbPath)
	var db *sql.DB
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(dbPath), os.ModeDir|0700); err != nil {
			return nil, err
		}

		fh, err := os.Create(dbPath) // Create SQLite file
		if err != nil {
			return nil, err
		}
		fh.Close()
		db, err = openAppDBNoValidate(dbPath, appName, schemaVersion)
		if err != nil {
			return nil, err
		}
		initSchema(db, appName, schemaVersion, schema)
	} else {
		db, err = OpenAppDB(dbPath, appName, schemaVersion)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func openAppDBNoValidate(dbPath string, appName string, schemaVersion uint8) (*sql.DB, error) {
	log.Println("openAppDBNoValidate(", dbPath, appName, schemaVersion, ")")
	var db *sql.DB
	filestat, err := os.Stat(dbPath)
	if err != nil {
		return nil, err
	}
	if filestat.Mode().IsRegular() {
		db, err = sql.Open("sqlite3", dbPath)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, os.ErrInvalid
	}
	return db, nil
}

func OpenAppDB(dbPath string, appName string, schemaVersion uint8) (*sql.DB, error) {
	log.Println("OpenAppDB(", dbPath, appName, schemaVersion, ")")
	db, err := openAppDBNoValidate(dbPath, appName, schemaVersion)
	if err != nil {
		return nil, err
	}
	err = validateDb(db, appName, schemaVersion)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// ExecSqlStatement prepares and executes one simple SQL statement and discards the result.
func ExecSqlStatement(db *sql.DB, sql string) error {
	log.Println("ExecSqlStatement(db,", sql, ")")
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

// ExecBulkSql prepares one SQL statement and executes it once for each set of values provides.
func ExecBulkSql(db *sql.DB, sql string, values []string) error {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for v := range values {
		_, err = stmt.Exec(values[v])
		if err != nil {
			return err
		}
	}
	return nil
}

func getUserVersion(appName string, schemaVersion uint8) uint32 {
	log.Println("getUserVersion(", appName, schemaVersion, ")")
	sum := sha256.Sum256([]byte(appName))
	s := []byte{sum[0], sum[1], sum[2], schemaVersion}
	uv := binary.LittleEndian.Uint32(s)
	return uv
}

func initSchema(db *sql.DB, appName string, schemaVersion uint8, schema []string) error {
	log.Println("initSchema(db,", appName, schemaVersion, ")")
	var s []string
	s = append(s, fmt.Sprintf("PRAGMA user_version = %d ;", getUserVersion(appName, schemaVersion)),
		`PRAGMA foreign_keys = ON;`)
	s = append(s, schema...)
	for v := range s {
		err := ExecSqlStatement(db, s[v])
		if err != nil {
			return &SchemaError{s[v], err}
		}
	}
	return nil
}

func validateDb(db *sql.DB, appName string, schemaVersion uint8) error {
	log.Println("validateDb(db,", appName, schemaVersion, ")")
	r := db.QueryRow("PRAGMA user_version")
	uv := getUserVersion(appName, schemaVersion)

	var user_version uint32

	if err := r.Scan(&user_version); err != nil {
		return err
	}
	if uv != user_version {
		var dbAppId uint32
		var expectedId uint32
		var dbSchemaVers uint8
		var expectedSchemaVers uint8
		dbAppId = user_version & 0x00ffffff
		expectedId = uv & 0x00ffffff
		dbSchemaVers = uint8(user_version >> 24)
		expectedSchemaVers = uint8(uv >> 24)
		if dbAppId != expectedId {
			return &AppIdError{dbAppId, expectedId}
		}
		if dbSchemaVers != expectedSchemaVers {
			return &SchemaVersionError{dbSchemaVers, expectedSchemaVers}
		}
	}
	return nil
}
