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

package org.apache.spark.sql.catalyst.expressions

import java.sql.Date

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Audit-only reproducer for mixed-sign CalendarInterval handling in sequence().
 *
 * The 28-day month approximation gives +2 months -61 days an estimated step of -5 days,
 * so Sequence treats this as descending.  Real calendar arithmetic is computed from the
 * start using plusMonths followed by plusDays, and is not monotonic in that direction.
 *
 * This test intentionally asserts the current (incorrect/non-monotonic) behavior so that it can
 * execute safely and demonstrate parity across interpreted evaluation, codegen, unsafe projection,
 * and optimizer folding through ExpressionEvalHelper.checkEvaluation.
 */
class SequenceMixedCalendarIntervalAuditSuite
  extends SparkFunSuite with ExpressionEvalHelper {

  test("audit: mixed calendar interval can move opposite to estimated sequence direction") {
    DateTimeTestUtils.withDefaultTimeZone(UTC) {
      val step = new CalendarInterval(2, -61, 0L)

      checkEvaluation(
        new Sequence(
          Literal(Date.valueOf("2020-12-01")),
          Literal(Date.valueOf("2020-11-30")),
          Literal(step)),
        Seq(
          Date.valueOf("2020-12-01"),
          Date.valueOf("2020-12-02"),
          Date.valueOf("2020-11-30"),
          Date.valueOf("2020-11-30"),
          Date.valueOf("2020-11-30"),
          Date.valueOf("2020-11-30"),
          Date.valueOf("2020-11-30"),
          Date.valueOf("2020-12-01")))
    }
  }
}
