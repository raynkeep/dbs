package dbs

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	*sql.DB
	dbName   string
	table    string
	fields   []string
	selector []Selector
	orderBy  []string
	skip     int64
	limit    int64
}

type S []Selector

type Selector struct {
	Field  string
	Symbol string
	Value  interface{}
}

type D []DocElem

type DocElem struct {
	Field string
	Value interface{}
}

func Open(dataSourceName string) (*DB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	LogInit()
	return &DB{DB: db}, err
}

func (db *DB) D(dbName string) *DB {
	db.dbName = dbName
	return db
}

func (db *DB) Table(name string) *DB {
	db.table = name
	return db
}

func (db *DB) Fields(fields []string) *DB {
	db.fields = fields
	return db
}

func (db *DB) Find(selector S) *DB {
	db.selector = selector
	return db
}

func (db *DB) Sort(orderBy []string) *DB {
	db.orderBy = orderBy
	return db
}

func (db *DB) Limit(limit int64) *DB {
	db.limit = limit
	return db
}

func (db *DB) Skip(skip int64) *DB {
	db.skip = skip
	return db
}

func (db *DB) Insert(data D) (id int64, err error) {
	kStr, vStr, args := GetSqlInsert(data)
	s := "INSERT INTO `" + db.table + "`(" + kStr + ") VALUES (" + vStr + ")"
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	id, err = res.LastInsertId()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (db *DB) Update(data D, where S) (n int64, err error) {
	setStr, args := GetSqlUpdate(data)
	whereStr, args2 := GetSqlWhere(where)
	args = append(args, args2...)

	s := "UPDATE `" + db.table + "` SET " + setStr + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	n, err = res.RowsAffected()
	return
}

func (db *DB) Delete(where S) (n int64, err error) {
	whereStr, args := GetSqlWhere(where)
	s := "DELETE FROM `" + db.table + "`" + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	n, err = res.RowsAffected()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (db *DB) Count(where S) (n int64, err error) {
	whereStr, args := GetSqlWhere(where)
	s := "SELECT COUNT(*) FROM `" + db.table + "`" + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	err = stmt.QueryRow(args...).Scan(&n)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (db *DB) One(scanArr []interface{}) (err error) {
	fields := GetSqlFields(db.fields)
	whereStr, args := GetSqlWhere(db.selector)
	s := "SELECT " + fields + " FROM `" + db.table + "`" + whereStr + " LIMIT 1"
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	err = stmt.QueryRow(args...).Scan(scanArr...)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLogWrite(err, s, args...)
		}
		return
	}
	return
}

func (db *DB) All(scanArr []interface{}, callback func()) (err error) {
	fields := GetSqlFields(db.fields)
	whereStr, args := GetSqlWhere(db.selector)
	orderStr := GetSqlOrderBy(db.orderBy)
	limitStr := GetSqlLimit(db.skip, db.limit)
	s := "SELECT " + fields + " FROM `" + db.table + "`" + whereStr + orderStr + limitStr
	LogWrite(s, args...)

	var rows *sql.Rows
	rows, err = db.Query(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(scanArr...)
		if err != nil {
			return
		}
		callback()
	}
	return
}

func (db *DB) OneMap() (row map[string]interface{}, columns []string, err error) {
	whereStr, args := GetSqlWhere(db.selector)
	fields := GetSqlFields(db.fields)
	s := "SELECT " + fields + " FROM `" + db.table + "`" + whereStr
	LogWrite(s, args...)

	var rows *sql.Rows
	rows, err = db.Query(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer rows.Close()

	if rows.Next() {
		m := map[string]interface{}{}
		columns, err = MapScan(rows, m)
		if err != nil {
			ErrorLogWrite(err, s, args...)
			return
		}
		row = m
	}
	return
}

func (db *DB) AllMap() (list []map[string]interface{}, columns []string, err error) {
	fields := GetSqlFields(db.fields)
	whereStr, args := GetSqlWhere(db.selector)
	orderStr := GetSqlOrderBy(db.orderBy)
	limitStr := GetSqlLimit(db.skip, db.limit)
	s := "SELECT " + fields + " FROM `" + db.table + "`" + whereStr + orderStr + limitStr
	LogWrite(s, args...)

	var rows *sql.Rows
	rows, err = db.Query(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer rows.Close()

	for rows.Next() {
		m := map[string]interface{}{}
		columns, err = MapScan(rows, m)
		if err != nil {
			ErrorLogWrite(err, s, args...)
			return
		}
		list = append(list, m)
	}
	return
}

// 绑定到 Map
func MapScan(r *sql.Rows, dest map[string]interface{}) (columns []string, err error) {
	columns, err = r.Columns()
	if err != nil {
		return
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	err = r.Err()
	return
}

func GetSqlData(data D) (fields []string, scanArr []interface{}) {
	for _, v := range data {
		fields = append(fields, v.Field)
		scanArr = append(scanArr, v.Value)
	}
	return
}

func GetSqlFields(fields []string) (s string) {
	if len(fields) > 0 {
		for _, field := range fields {
			s += "`" + field + "`,"
		}
		s = strings.TrimRight(s, ",")
	} else {
		s = "*"
	}
	return
}

func GetSqlOrderBy(arr []string) (s string) {
	if len(arr) > 0 {
		s = " ORDER BY "
		for _, v := range arr {
			v = strings.Replace(v, " ", "", -1)
			symbol := v[0:1]
			if symbol == "-" {
				s += "`" + v[1:] + "` DESC,"
			} else {
				s += "`" + v + "` ASC,"
			}
		}
		s = strings.TrimRight(s, ",")
	}
	return
}

func GetSqlLimit(skip, limit int64) (s string) {
	if limit > 0 {
		s = " LIMIT " + strconv.FormatInt(skip, 10) + "," + strconv.FormatInt(limit, 10)
	}
	return
}

func GetSqlInsert(data D) (kStr, vStr string, args []interface{}) {
	for _, v := range data {
		kStr += "`" + v.Field + "`, "
		vStr += "?, "
		args = append(args, v.Value)
	}
	kStr = strings.TrimSuffix(kStr, ", ")
	vStr = strings.TrimSuffix(vStr, ", ")
	return
}

func GetSqlUpdate(data D) (setStr string, args []interface{}) {
	for _, v := range data {
		symbol := v.Field[0:1]
		if symbol == "+" || symbol == "-" {
			field := v.Field[1:]
			setStr += "`" + field + "`=`" + field + "`" + symbol + "?, "
		} else {
			setStr += "`" + v.Field + "`=?, "
		}
		args = append(args, v.Value)
	}
	setStr = strings.TrimSuffix(setStr, ", ")
	return
}

func GetSqlWhere(selector S) (whereStr string, args []interface{}) {
	if len(selector) == 0 {
		return
	}
	whereStr = " WHERE "
	for _, v := range selector {
		if v.Symbol == "IN" {
			s2 := ""
			switch t := v.Value.(type) {
			case []int:
				arr := v.Value.([]int)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			case []int64:
				arr := v.Value.([]int64)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			case []string:
				arr := v.Value.([]string)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			default:
				fmt.Println("Unsupported types:", t)
			}
			if s2 != "" {
				s2 = strings.Trim(s2, ",")
				whereStr += "`" + v.Field + "` IN (" + s2 + ") AND "
			}
		} else {
			whereStr += "`" + v.Field + "` " + v.Symbol + " ? AND "
			args = append(args, v.Value)
		}
	}
	whereStr = strings.TrimSuffix(whereStr, " AND ")
	return
}

var LogFile string
var ErrorLogFile string

var LogIoWriter io.Writer = os.Stdout

func LogInit() {
	if LogFile != "" {
		var err error
		LogIoWriter, err = os.OpenFile(LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
	}
}

func LogWrite(s string, args ...interface{}) {
	if LogFile == "" {
		return
	}
	_, err := fmt.Fprintf(LogIoWriter, "%v | %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		fmt.Sprintf(strings.Replace(s, "?", "'%v'", -1), ReplaceSlash(args...)...),
	)
	if err != nil {
		fmt.Println(err)
	}
}

func ErrorLogWrite(e error, s string, args ...interface{}) {
	if ErrorLogFile == "" {
		return
	}

	f, err := os.OpenFile(ErrorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	str := fmt.Sprintf("%v | ERROR: %v | SQL: %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		e.Error(),
		fmt.Sprintf(strings.Replace(s, "?", "'%v'", -1), ReplaceSlash(args...)...),
	)
	if _, err := f.Write([]byte(str)); err != nil {
		fmt.Println(err)
	}

	if err := f.Close(); err != nil {
		fmt.Println(err)
	}
}

func ReplaceSlash(args ...interface{}) []interface{} {
	for k := range args {
		if s, ok := args[k].(string); ok {
			s = strings.Replace(s, "'", "\\'", -1)
			s = strings.Replace(s, "\\", "\\\\", -1)
			args[k] = s
		}
	}
	return args
}
