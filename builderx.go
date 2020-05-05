package builderx

import (
	"errors"

	"github.com/iancoleman/strcase"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

// DB .
type DB interface {
	Builder() sq.StatementBuilderType

	Get(dest interface{}, sql interface{}, args ...interface{}) error
	Select(dest interface{}, sql interface{}, args ...interface{}) error
	Exec(sql interface{}, args ...interface{}) error
}

type queryable interface {
	ToSql() (string, []interface{}, error)
}

type dbWrap struct {
	conn *sqlx.DB
}

// Connect .
func Connect(driverName, dns string) (DB, error) {

	db, err := sqlx.Connect(driverName, dns)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	db.Mapper = reflectx.NewMapperFunc("db", strcase.ToSnake)

	wrapper := &dbWrap{conn: db}
	return wrapper, nil
}

// Builder .
func (db *dbWrap) Builder() sq.StatementBuilderType {
	return sq.StatementBuilderType{}.
		RunWith(db.conn).
		PlaceholderFormat(sq.Dollar)
}

// Select .
func (db *dbWrap) Select(dest interface{}, sql interface{}, args ...interface{}) (err error) {
	sqlString, args, err := stringifyQuery(sql, args...)
	if err != nil {
		return err
	}

	return db.conn.Unsafe().Select(dest, sqlString, args...)
}

// Get .
func (db *dbWrap) Get(dest interface{}, sql interface{}, args ...interface{}) (err error) {
	sqlString, args, err := stringifyQuery(sql, args...)
	if err != nil {
		return err
	}

	return db.conn.Unsafe().Get(dest, sqlString, args...)
}

// Exec .
func (db *dbWrap) Exec(sql interface{}, args ...interface{}) (err error) {
	sqlString, args, err := stringifyQuery(sql, args...)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(sqlString, args...)
	return err
}

func stringifyQuery(sql interface{}, args ...interface{}) (string, []interface{}, error) {
	query, ok := sql.(queryable)
	if ok {
		return query.ToSql()
	}

	sqlString, ok := sql.(string)
	if !ok {
		return "", args, errors.New("sql param is invalid")
	}

	return sqlString, args, nil
}
