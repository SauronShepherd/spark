/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.mockito.Mockito.{doAnswer, spy}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class JdbcDriverConnectionSuite extends SharedSparkSession {
  import testImplicits._

  test("SPARK-51264: close driver connection before starting JDBC write") {
    val url = "jdbc:h2:mem:spark51264;DB_CLOSE_DELAY=-1"
    Utils.classForName("org.h2.Driver")

    val setupConn = DriverManager.getConnection(url)
    try {
      setupConn.createStatement().executeUpdate(
        "CREATE TABLE test_table (id BIGINT NOT NULL)")
    } finally {
      setupConn.close()
    }

    val driverConnectionClosed = new AtomicBoolean(false)
    val connectionCount = new AtomicInteger(0)
    val testDialect = new JdbcDialect {
      override def canHandle(jdbcUrl: String): Boolean = jdbcUrl == url

      override def createConnectionFactory(options: JDBCOptions): Int => Connection = { _ =>
        val connection = DriverManager.getConnection(options.url)
        if (connectionCount.getAndIncrement() == 0) {
          val driverConnection = spy(connection)
          doAnswer { invocation =>
            driverConnectionClosed.set(true)
            invocation.callRealMethod()
          }.when(driverConnection).close()
          driverConnection
        } else {
          connection
        }
      }
    }

    JdbcDialects.unregisterDialect(H2Dialect())
    JdbcDialects.registerDialect(testDialect)
    try {
      val df = spark.range(1).mapPartitions { rows =>
        assert(driverConnectionClosed.get(),
          "driver JDBC connection should be closed before the distributed write starts")
        rows
      }.toDF("id")

      df.write.mode(SaveMode.Append).jdbc(url, "test_table", new Properties())

      val checkConn = DriverManager.getConnection(url)
      try {
        val rs = checkConn.createStatement().executeQuery("SELECT COUNT(*) FROM test_table")
        assert(rs.next())
        assert(rs.getLong(1) == 1L)
      } finally {
        checkConn.close()
      }
    } finally {
      JdbcDialects.unregisterDialect(testDialect)
      JdbcDialects.registerDialect(H2Dialect())
    }
  }
}
