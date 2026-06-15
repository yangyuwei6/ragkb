package mysql

import (
	"errors"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// IsDuplicateKey 判断错误是否为 MySQL 唯一键冲突（errno 1062）。
func IsDuplicateKey(err error) bool {
	var me *mysqlDriver.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1062
	}
	return false
}
