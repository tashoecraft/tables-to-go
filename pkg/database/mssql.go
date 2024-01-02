package database

import (
	"database/sql"
	"fmt"
	"github.com/fraenky8/tables-to-go/pkg/settings"
	"strings"
)

type MsSQL struct {
	*GeneralDatabase

	defaultUserName string
}

func NewMssql(s *settings.Settings) *MsSQL {
	return &MsSQL{
		GeneralDatabase: &GeneralDatabase{
			Settings: s,
			driver:   dbTypeToDriverMap[s.DbType],
		},
		defaultUserName: "root",
	}
}

func (mssql *MsSQL) DSN() string {
	user := mssql.defaultUserName
	if mssql.Settings.User != "" {
		user = mssql.Settings.User
	}
	return fmt.Sprintf("server=%s;port=%s;User ID=%s;password=%s;database=Powerlink;%s", mssql.Settings.Host, mssql.Settings.Port, user, mssql.Settings.Pswd, "encrypt=disable;")
}

func (mssql *MsSQL) Connect() (err error) {
	return mssql.GeneralDatabase.Connect(mssql.DSN())
}

func (mssql *MsSQL) GetTables() (tables []*Table, err error) {
	err = mssql.Select(&tables, `
    SELECT table_name AS table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema = 'dbo'
    ORDER BY table_name
`, mssql.DbName)

	if mssql.Verbose {
		if err != nil {
			fmt.Println("> Error at GetTables()")
			fmt.Printf("> schema: %q\r\n", mssql.DbName)
		}
	}

	return tables, err
}

func (mssql *MsSQL) PrepareGetColumnsOfTableStmt() (err error) {

	mssql.GetColumnsOfTableStmt, err = mssql.Preparex(`
        SELECT
          ordinal_position,
          column_name,
          data_type,
          column_default,
          is_nullable,
          character_maximum_length,
          numeric_precision
          -- Note: MSSQL doesn't have direct equivalents for 'column_key' and 'extra'
        FROM information_schema.columns
        WHERE table_name = @TableName
        ORDER BY ordinal_position
    `)
	return err
}

func (mssql *MsSQL) GetColumnsOfTable(table *Table) (err error) {
	err = mssql.GetColumnsOfTableStmt.Select(&table.Columns, sql.Named("TableName", table.Name))
	if mssql.Settings.Verbose {
		if err != nil {
			fmt.Printf("> Error at GetColumnsOfTable(%v)\r\n", table.Name)
			fmt.Printf("> schema: %q\r\n", mssql.Schema)
			fmt.Printf("> dbName: %q\r\n", mssql.DbName)
		}
	}

	return err
}

func (mssql *MsSQL) GetViews() (views []*Table, err error) {
	err = mssql.Select(&views, `
  	SELECT table_name AS table_name
        FROM information_schema.views
        WHERE table_schema = 'dbo'
        ORDER BY table_name
    `, mssql.DbName)

	if mssql.Verbose {
		if err != nil {
			fmt.Println("> Error at GetViews()")
			fmt.Printf("> schema: %q\r\n", mssql.DbName)
		}
	}

	return views, err
}

func (mssql *MsSQL) PrepareGetColumnsOfViewStmt() (err error) {
	mssql.GetColumnsOfViewStmt, err = mssql.Preparex(`
        SELECT
          ordinal_position,
          column_name,
          data_type,
          column_default,
          is_nullable,
          character_maximum_length,
          numeric_precision
        FROM information_schema.columns
        WHERE table_name = @ViewName
        ORDER BY ordinal_position
    `)
	return err
}

func (mssql *MsSQL) GetColumnsOfView(view *Table) (err error) {
	err = mssql.GetColumnsOfViewStmt.Select(&view.Columns, sql.Named("ViewName", view.Name))
	if mssql.Settings.Verbose {
		if err != nil {
			fmt.Printf("> Error at GetColumnsOfView(%v)\r\n", view.Name)
			fmt.Printf("> schema: %q\r\n", mssql.Schema)
			fmt.Printf("> dbName: %q\r\n", mssql.DbName)
		}
	}

	return err
}

func (mssql *MsSQL) IsPrimaryKey(column Column) bool {
	return strings.Contains(column.ColumnKey, "PRI")
}

func (mssql *MsSQL) IsAutoIncrement(column Column) bool {
	return strings.Contains(column.Extra, "auto_increment")
}

func (mssql *MsSQL) GetStringDatatypes() []string {
	return []string{
		"char",
		"varchar",
		"binary",
		"varbinary",
	}
}

func (mssql *MsSQL) IsString(column Column) bool {
	return isStringInSlice(column.DataType, mssql.GetStringDatatypes())
}

func (mssql *MsSQL) GetTextDatatypes() []string {
	return []string{
		"text",
		"blob",
	}
}

func (mssql *MsSQL) IsText(column Column) bool {
	return isStringInSlice(column.DataType, mssql.GetTextDatatypes())
}

func (mssql *MsSQL) GetIntegerDatatypes() []string {
	return []string{
		"tinyint",
		"smallint",
		"mediumint",
		"int",
		"bigint",
	}
}

func (mssql *MsSQL) IsInteger(column Column) bool {
	return isStringInSlice(column.DataType, mssql.GetIntegerDatatypes())
}

func (mssql *MsSQL) GetFloatDatatypes() []string {
	return []string{
		"numeric",
		"decimal",
		"float",
		"real",
	}
}

func (mssql *MsSQL) IsFloat(column Column) bool {
	return isStringInSlice(column.DataType, mssql.GetFloatDatatypes())
}

func (mssql *MsSQL) GetTemporalDatatypes() []string {
	return []string{
		"time",
		"datetimeoffset",
		"date",
		"datetime",
		"datetime2",
		"smalldatetime",
	}
}

func (mssql *MsSQL) IsTemporal(column Column) bool {
	return isStringInSlice(column.DataType, mssql.GetTemporalDatatypes())
}
