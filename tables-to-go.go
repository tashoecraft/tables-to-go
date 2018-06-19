package main

import (
	"flag"
	"fmt"
	"go/format"
	"io/ioutil"
	"os"
	"strings"

	"github.com/fraenky8/tables-to-go/pkg"
	"github.com/fraenky8/tables-to-go/pkg/database/mysql"
	"github.com/fraenky8/tables-to-go/pkg/database/postgresql"
	"github.com/fraenky8/tables-to-go/pkg/tagger"
)

var (
	// map of Tagger used
	// key is a ascending sequence of i*2 to determine which tags to generate later
	taggers = map[int]pkg.Tagger{
		1: new(tagger.Db),
		2: new(tagger.Mastermind),
		4: new(tagger.SQL),
	}

	// means that the `db`-Tag is enabled by default
	effectiveTags = 1
)

// cmdArgs represents the supported command line args
type cmdArgs struct {
	Help bool
	*pkg.Settings
}

// newCmdArgs creates and prepares the command line arguments with default values
func newCmdArgs() (args *cmdArgs) {

	args = &cmdArgs{
		Settings: pkg.NewSettings(),
	}

	flag.BoolVar(&args.Help, "?", false, "shows help and usage")
	flag.BoolVar(&args.Help, "help", false, "shows help and usage")
	flag.BoolVar(&args.Verbose, "v", args.Verbose, "verbose output")
	flag.StringVar(&args.DbType, "t", args.DbType, fmt.Sprintf("type of database to use, currently supported: %v", args.SupportedDbTypes()))
	flag.StringVar(&args.User, "u", args.User, "user to connect to the database")
	flag.StringVar(&args.Pswd, "p", args.Pswd, "password of user")
	flag.StringVar(&args.DbName, "d", args.DbName, "database name")
	flag.StringVar(&args.Schema, "s", args.Schema, "schema name")
	flag.StringVar(&args.Host, "h", args.Host, "host of database")
	flag.StringVar(&args.Port, "port", args.Port, "port of database host, if not specified, it will be the default ports for the supported databases")

	flag.StringVar(&args.OutputFilePath, "of", args.OutputFilePath, "output file path, default is current working directory")
	flag.StringVar(&args.OutputFormat, "format", args.OutputFormat, "camelCase (c) or original (o)")
	flag.StringVar(&args.Prefix, "pre", args.Prefix, "prefix for file- and struct names")
	flag.StringVar(&args.Suffix, "suf", args.Suffix, "suffix for file- and struct names")
	flag.StringVar(&args.PackageName, "pn", args.PackageName, "package name")

	flag.BoolVar(&args.TagsNoDb, "tags-no-db", args.TagsNoDb, "do not create db-tags")

	flag.BoolVar(&args.TagsMastermindStructable, "tags-structable", args.TagsMastermindStructable, "generate struct with tags for use in Masterminds/structable (https://github.com/Masterminds/structable)")
	flag.BoolVar(&args.TagsMastermindStructableOnly, "tags-structable-only", args.TagsMastermindStructableOnly, "generate struct with tags ONLY for use in Masterminds/structable (https://github.com/Masterminds/structable)")
	flag.BoolVar(&args.IsMastermindStructableRecorder, "structable-recorder", args.IsMastermindStructableRecorder, "generate a structable.Recorder field")

	flag.BoolVar(&args.TagsSQL, "experimental-tags-sql", args.TagsSQL, "generate struct with sql-tags")
	flag.BoolVar(&args.TagsSQLOnly, "experimental-tags-sql-only", args.TagsSQLOnly, "generate struct with ONLY sql-tags")

	flag.Parse()

	return args
}

// main function to run the transformations
func main() {

	cmdArgs := newCmdArgs()

	if cmdArgs.Help {
		flag.Usage()
		os.Exit(0)
	}

	if err := cmdArgs.Verify(); err != nil {
		fmt.Printf("settings verification error: %v", err)
		os.Exit(1)
	}

	createEffectiveTags(cmdArgs.Settings)

	gdb := &pkg.GeneralDatabase{
		Settings: cmdArgs.Settings,
	}

	var db pkg.Database

	switch cmdArgs.DbType {
	case "mysql":
		db = &mysql.Mysql{gdb}
	case "postgres":
	default:
		db = &postgresql.Postgresql{gdb}
	}

	if err := db.Connect(); err != nil {
		fmt.Printf("could not connect to database: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := run(cmdArgs.Settings, db); err != nil {
		fmt.Printf("run error: %v", err)
		os.Exit(1)
	}
}

func createEffectiveTags(settings *pkg.Settings) {
	if settings.TagsNoDb {
		effectiveTags = 0
	}
	if settings.TagsMastermindStructable {
		effectiveTags |= 2
	}
	if settings.TagsMastermindStructableOnly {
		effectiveTags = 0
		effectiveTags |= 2
	}
	if settings.TagsSQL {
		effectiveTags |= 4
	}
	if settings.TagsSQLOnly {
		effectiveTags = 0
		effectiveTags |= 4
	}
	// last tag-"ONLY" wins if multiple specified
}

func run(settings *pkg.Settings, db pkg.Database) (err error) {

	fmt.Printf("running for %q...\r\n", settings.DbType)

	tables, err := db.GetTables()
	if err != nil {
		return fmt.Errorf("could not get tables: %v", err)
	}

	if settings.Verbose {
		fmt.Printf("> number of tables: %v\r\n", len(tables))
	}

	if err = db.PrepareGetColumnsOfTableStmt(); err != nil {
		return fmt.Errorf("could not prepare the get-column-statement: %v", err)
	}

	for _, table := range tables {

		if settings.Verbose {
			fmt.Printf("> processing table %q\r\n", table.Name)
		}

		if err = db.GetColumnsOfTable(table); err != nil {
			return fmt.Errorf("could not get columns of table %s: %v", table.Name, err)
		}

		if settings.Verbose {
			fmt.Printf("\t> number of columns: %v\r\n", len(table.Columns))
		}

		tableName, content := createTableStructString(settings, db, table)

		if err = createStructFile(settings.OutputFilePath, tableName, content); err != nil {
			return fmt.Errorf("could not create struct file for table %s: %v", table.Name, err)
		}
	}

	fmt.Println("done!")

	return err
}

func createTableStructString(settings *pkg.Settings, db pkg.Database, table *pkg.Table) (string, string) {

	var structFields strings.Builder

	var isNullable bool
	var isTime bool

	for _, column := range table.Columns {

		// TODO add verbosity levels
		// if settings.Verbose {
		// 	fmt.Printf("\t> %v\r\n", column.Name)
		// }

		column.Name = strings.Title(column.Name)
		if settings.OutputFormat == "c" {
			column.Name = camelCaseString(column.Name)
		}
		columnType, isTimeType := mapDbColumnTypeToGoType(db, column)

		structFields.WriteString(column.Name)
		structFields.WriteString(" ")
		structFields.WriteString(columnType)
		structFields.WriteString(generateTags(db, column))
		structFields.WriteString("\n")

		// save some info for later use
		if column.IsNullable == "YES" {
			isNullable = true
		}
		if isTimeType {
			isTime = true
		}
	}

	if settings.IsMastermindStructableRecorder {
		structFields.WriteString("\t\nstructable.Recorder\n")
	}

	var fileContent strings.Builder

	// write header infos
	fileContent.WriteString("package ")
	fileContent.WriteString(settings.PackageName)
	fileContent.WriteString("\n\n")

	// do imports
	if isNullable || isTime || settings.IsMastermindStructableRecorder {
		fileContent.WriteString("import (\n")

		if isNullable {
			fileContent.WriteString("\t\"database/sql\"\n")
		}

		if isTime {
			if isNullable {
				fileContent.WriteString("\t\n\"github.com/lib/pq\"\n")
			} else {
				fileContent.WriteString("\t\"time\"\n")
			}
		}

		if settings.IsMastermindStructableRecorder {
			fileContent.WriteString("\t\n\"github.com/Masterminds/structable\"\n")
		}

		fileContent.WriteString(")\n\n")
	}

	tableName := strings.Title(settings.Prefix + table.Name + settings.Suffix)
	if settings.OutputFormat == "c" {
		tableName = camelCaseString(tableName)
	}

	// write struct with fields
	fileContent.WriteString("type ")
	fileContent.WriteString(tableName)
	fileContent.WriteString(" struct {\n")
	fileContent.WriteString(structFields.String())
	fileContent.WriteString("}")

	return tableName, fileContent.String()
}

func createStructFile(path, name, content string) error {

	fileName := path + name + ".go"

	// format it
	formatedContent, err := format.Source([]byte(content))
	if err != nil {
		return fmt.Errorf("could not format file %s: %v", fileName, err)
	}

	return ioutil.WriteFile(fileName, formatedContent, 0666)
}

func generateTags(db pkg.Database, column pkg.Column) (tags string) {
	for t := 1; t <= effectiveTags; t *= 2 {
		shouldTag := effectiveTags&t > 0
		if shouldTag {
			tags += taggers[t].GenerateTag(db, column) + " "
		}
	}
	if len(tags) > 0 {
		tags = " `" + strings.TrimSpace(tags) + "`"
	}
	return tags
}

func mapDbColumnTypeToGoType(db pkg.Database, column pkg.Column) (goType string, isTime bool) {

	isTime = false

	if db.IsString(column) || db.IsText(column) {
		goType = "string"
		if db.IsNullable(column) {
			goType = "sql.NullString"
		}
	} else if db.IsInteger(column) {
		goType = "int"
		if db.IsNullable(column) {
			goType = "sql.NullInt64"
		}
	} else if db.IsFloat(column) {
		goType = "float64"
		if db.IsNullable(column) {
			goType = "sql.NullFloat64"
		}
	} else if db.IsTemporal(column) {
		goType = "time.Time"
		if db.IsNullable(column) {
			goType = "pq.NullTime"
		}
		isTime = true
	} else {

		// TODO handle special data types
		switch column.DataType {
		case "boolean":
			goType = "bool"
			if db.IsNullable(column) {
				goType = "sql.NullBool"
			}
		default:
			goType = "sql.NullString"
		}
	}

	return goType, isTime
}

func camelCaseString(s string) (cc string) {
	splitted := strings.Split(s, "_")

	if len(splitted) == 1 {
		return strings.Title(s)
	}

	for _, part := range splitted {
		cc += strings.Title(strings.ToLower(part))
	}
	return cc
}
