from pathlib import Path

provider = Path("sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcRelationProvider.scala")
text = provider.read_text()
text = text.replace("import org.apache.spark.sql.jdbc.JdbcDialects\n", "")
old = '''    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            }

          case SaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)

          case SaveMode.ErrorIfExists =>
            throw QueryCompilationErrors.tableOrViewAlreadyExistsError(options.table)

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }
'''
new = '''    val tableSchemaToWrite = JdbcUtils.withConnection(options) { conn =>
      if (JdbcUtils.tableExists(conn, options)) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              Some(JdbcUtils.getSchemaOption(conn, options))
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              Some(Some(df.schema))
            }

          case SaveMode.Append =>
            Some(JdbcUtils.getSchemaOption(conn, options))

          case SaveMode.ErrorIfExists =>
            throw QueryCompilationErrors.tableOrViewAlreadyExistsError(options.table)

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
            None
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        Some(Some(df.schema))
      }
    }

    tableSchemaToWrite.foreach { tableSchema =>
      saveTable(df, tableSchema, isCaseSensitive, options)
    }
'''
if old not in text:
    raise SystemExit("JdbcRelationProvider expected block not found")
provider.write_text(text.replace(old, new))

suite = Path("sql/core/src/test/scala/org/apache/spark/sql/jdbc/JDBCWriteSuite.scala")
text = suite.read_text()
text = text.replace(
    "import java.sql.{Date, DriverManager, Timestamp}\n",
    "import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}\n"
    "import java.sql.{Connection, Date, DriverManager, Timestamp}\n")
text = text.replace(
    "import org.apache.spark.SparkException\n",
    "import org.apache.spark.{SparkException, TaskContext}\n")

class_marker = "class JDBCWriteSuite extends SharedSparkSession with BeforeAndAfter {\n"
companion = '''object JDBCWriteSuite {
  @volatile var driverConnectionClosed = false

  def trackingDialect(testUrl: String): JdbcDialect = new JdbcDialect {
    override def canHandle(jdbcUrl: String): Boolean = jdbcUrl == testUrl

    override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

    override def createConnectionFactory(options: JDBCOptions): Int => Connection = { _ =>
      if (TaskContext.get() != null) {
        assert(driverConnectionClosed,
          "driver JDBC connection should be closed before the distributed write starts")
        DriverManager.getConnection(options.url)
      } else {
        val connection = DriverManager.getConnection(options.url)
        val handler = new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            val methodArgs = if (args == null) Array.empty[AnyRef] else args
            try {
              val result = method.invoke(connection, methodArgs: _*)
              if (method.getName == "close") {
                driverConnectionClosed = true
              }
              result
            } catch {
              case e: InvocationTargetException => throw e.getCause
            }
          }
        }
        Proxy.newProxyInstance(
          connection.getClass.getClassLoader,
          Array(classOf[Connection]),
          handler).asInstanceOf[Connection]
      }
    }
  }
}

'''
if class_marker not in text:
    raise SystemExit("JDBCWriteSuite class marker not found")
text = text.replace(class_marker, companion + class_marker, 1)

test_marker = '  test("createTableOptions") {\n'
regression_test = '''  test("SPARK-51264: close driver connection before starting JDBC write") {
    val trackingDialect = JDBCWriteSuite.trackingDialect(url)
    val df = spark.createDataFrame(sparkContext.parallelize(arr1x2), schema2)
    val tables = Seq(
      "TEST.SPARK51264_APPEND",
      "TEST.SPARK51264_TRUNCATE",
      "TEST.SPARK51264_OVERWRITE",
      "TEST.SPARK51264_CREATE")

    def dropTable(table: String): Unit = {
      val statement = conn.createStatement()
      try {
        statement.executeUpdate(s"DROP TABLE IF EXISTS $table")
      } finally {
        statement.close()
      }
    }

    def resetTable(table: String): Unit = {
      dropTable(table)
      val statement = conn.createStatement()
      try {
        statement.executeUpdate(s"CREATE TABLE $table (name TEXT(32), id INTEGER)")
      } finally {
        statement.close()
      }
    }

    def writeAndCheck(table: String, mode: SaveMode, truncate: Boolean = false): Unit = {
      JDBCWriteSuite.driverConnectionClosed = false
      val writer = df.write.mode(mode)
      if (truncate) {
        writer.option("truncate", true)
      }
      writer.jdbc(url, table, new Properties())
      assert(JDBCWriteSuite.driverConnectionClosed)
    }

    JdbcDialects.unregisterDialect(H2Dialect())
    JdbcDialects.registerDialect(trackingDialect)
    try {
      resetTable(tables(0))
      writeAndCheck(tables(0), SaveMode.Append)

      resetTable(tables(1))
      writeAndCheck(tables(1), SaveMode.Overwrite, truncate = true)

      resetTable(tables(2))
      writeAndCheck(tables(2), SaveMode.Overwrite)

      dropTable(tables(3))
      writeAndCheck(tables(3), SaveMode.Append)
    } finally {
      try {
        tables.foreach(dropTable)
      } finally {
        JDBCWriteSuite.driverConnectionClosed = false
        JdbcDialects.unregisterDialect(trackingDialect)
        JdbcDialects.registerDialect(H2Dialect())
      }
    }
  }

'''
if test_marker not in text:
    raise SystemExit("JDBCWriteSuite insertion marker not found")
suite.write_text(text.replace(test_marker, regression_test + test_marker, 1))
