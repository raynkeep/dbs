package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ryankeep/dbs"
)

type User struct {
	Uid        int64  `db:"uid,auto_increment"`
	Gid        int64  `db:"gid"`
	Name       string `db:"name"`
	CreateDate string `db:"createDate"`
}

var err error
var db *dbs.DB

func main() {
	os.Remove("./test.db")

	// 开启日志
	dbs.LogFile = "./db.log"
	dbs.ErrorLogFile = "./db.error.log"

	// 打开数据库
	db, err = dbs.Open("sqlite3", "./test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 创建表
	_, err = db.Exec(`DROP TABLE IF EXISTS user;
CREATE TABLE user
(
  uid        INTEGER PRIMARY KEY AUTOINCREMENT,
  gid        INTEGER NOT NULL DEFAULT '0',
  name       TEXT             DEFAULT '',
  createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
);`)
	if err != nil {
		panic(err)
	}

	// 参数设置
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(2000)
	db.SetConnMaxLifetime(time.Second * 5)

	// 插入
	for i := 1; i <= 5; i++ {
		uid, err := db.Table("user").Insert(dbs.D{
			{"gid", 1},
			{"name", "admin" + strconv.Itoa(i)},
			{"createDate", time.Now().Format("2006-01-02 15:04:05")},
		})
		if err != nil {
			panic(err)
		}
		fmt.Println("Insert:", uid)
	}

	for i := 1; i <= 5; i++ {
		st := User{}
		st.Gid = 3
		st.Name = "twoTest" + strconv.Itoa(i)
		st.CreateDate = time.Now().Format("2006-01-02 15:04:05")
		uid, err := db.Table("user").InsertS(st)
		if err != nil {
			panic(err)
		}
		fmt.Println("Insert:", uid)
	}

	// 更新
	n, err := db.Table("user").Update(dbs.D{
		{"gid", 2},
		{"name", "test"},
	}, dbs.S{
		{"uid", "=", 1},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Update:", n)

	// 统计数量
	n, err = db.Table("user").Count(dbs.S{})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Count:", n)

	// ===========================================================================
	// 映射结构体
	scanF := func() (ptr *User, fields []string, args *[]interface{}) {
		row := User{}
		fields, scanArr := dbs.GetSqlData(dbs.D{
			{"uid", &row.Uid},
			{"gid", &row.Gid},
			{"name", &row.Name},
			{"createDate", &row.CreateDate},
		})
		ptr = &row
		args = &scanArr
		return
	}
	data, fields, scanArr := scanF()

	// 读取一条 到 结构体
	err = db.Table("user").Fields(fields).Find(dbs.S{
		{"uid", "=", 1},
	}).One(*scanArr)
	if err != nil {
		panic(err)
	}
	u := *data
	fmt.Println()
	fmt.Printf("Read: %+v\n", u)

	// 读取多条 到 结构体
	var list []User
	err = db.Table("user").Fields(fields).Sort([]string{"-gid", "-uid"}).Skip(2).Limit(2).Find(dbs.S{
		{"uid", "<", 5},
	}).All(*scanArr, func() {
		list = append(list, *data)
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("List:", list)
	listByte, _ := json.Marshal(list)
	fmt.Println("Json:", string(listByte))

	// 读取一条 到 Map
	rowMap, columns, err := db.Table("user").Find(dbs.S{
		{"uid", "=", 1},
	}).OneMap()
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("row Map:", rowMap)
	rowMapByte, _ := json.Marshal(rowMap)
	fmt.Println("Json Map:", string(rowMapByte))
	fmt.Println("columns:", columns)

	// 读取多条 到 Map
	listMap, columns, err := db.Table("user").Find(dbs.S{
		{"uid", "<", 5},
	}).AllMap()
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("List Map:", listMap)
	listMapByte, _ := json.Marshal(listMap)
	fmt.Println("Json Map:", string(listMapByte))
	fmt.Println("columns:", columns)

	// 删除
	n, err = db.Table("user").Delete(dbs.S{
		{"uid", "=", 9},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Delete:", n)
}
