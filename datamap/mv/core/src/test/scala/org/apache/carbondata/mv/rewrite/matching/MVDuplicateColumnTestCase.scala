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

package org.apache.carbondata.mv.rewrite.matching

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.rewrite.TestUtil

class MVDuplicateColumnTestCase extends QueryTest with BeforeAndAfterAll {

  private val timestampFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    drop()
    sql("CREATE TABLE maintable (empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        "deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int, utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE maintable  OPTIONS
           |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }

  def drop(): Unit = {
    sql("drop table IF EXISTS maintable")
  }

  override def afterAll(): Unit = {
    drop()
    if (null != timestampFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
    }
  }

  test("test mv queries with duplicate columns") {
    val res1 = sql("select empname,empname as b,projectcode from maintable group by empname,projectcode")
    val res2 =  sql("select empname,sum(projectcode),empname as b,empname as c from maintable group by empname")
    val res3 = sql("select empname,sum(projectcode),empname,empname from maintable group by empname")
    val res4 = sql("select empname,sum(projectcode),sum(projectcode) from maintable group by empname")
    sql("drop datamap if exists datamap1")
    sql("create datamap datamap1 using 'mv' as select empname,projectcode from maintable group by empname,projectcode")
    val analyzed1 = sql("select empname,empname as b,projectcode from maintable group by empname,projectcode").queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap1"))
    checkAnswer(sql("select empname,empname as b,projectcode from maintable group by empname,projectcode"), res1)
    val analyzed2 = sql("select empname,sum(projectcode),empname as b,empname as c from maintable group by empname").queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed2, "datamap1"))
    checkAnswer(sql("select empname,sum(projectcode),empname as b,empname as c from maintable group by empname"), res2)
    val analyzed3 = sql("select empname,sum(projectcode),empname,empname from maintable group by empname").queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed3, "datamap1"))
    checkAnswer(sql("select empname,sum(projectcode),empname,empname from maintable group by empname"), res3)
    val analyzed4 = sql("select empname,sum(projectcode),sum(projectcode) from maintable group by empname").queryExecution.analyzed
    assert(TestUtil.verifyMVDataMap(analyzed4, "datamap1"))
    checkAnswer(sql("select empname,sum(projectcode),sum(projectcode) from maintable group by empname"), res4)
    sql("drop datamap if exists datamap1")
  }

  test("test mv queries with duplicate columns without group by") {
//    val res1 = sql("select empname,empname as b,projectcode from maintable")
//    val res2 =  sql("select empname,projectcode,empname as b,empname as c from maintable")
//    val res3 = sql("select empname,projectcode,empname,empname from maintable")
//    val res4 = sql("select empname,projectcode,projectcode from maintable")
    sql("drop datamap if exists datamap1")
    sql("create datamap datamap1 using 'mv' as select empname,projectcode from maintable ")
//    val analyzed1 = sql("select empname,empname as b,projectcode from maintable ").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap1"))
//    checkAnswer(sql("select empname,empname as b,projectcode from maintable "), res1)
//    val analyzed2 = sql("select empname,projectcode,empname as b,empname as c from maintable").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed2, "datamap1"))
//    checkAnswer(sql("select empname,projectcode,empname as b,empname as c from maintable"), res2)
//    val analyzed3 = sql("select empname,projectcode,empname,empname from maintable").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed3, "datamap1"))
//    checkAnswer(sql("select empname,projectcode,empname,empname from maintable"), res3)
//    val analyzed4 = sql("select empname,projectcode,projectcode from maintable ").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed4, "datamap1"))
//    checkAnswer(sql("select empname,projectcode,projectcode from maintable"), res4)
//    sql("drop datamap if exists datamap1")

    sql("select empname,projectcode,empname,empname as b from maintable").show(false)
  }

  test("test mv datamap with duplicate columns in create without group by") {
//    val res1 = sql("select empname,empname,projectcode from maintable")
//    val res2 = sql("select empname,projectcode from maintable")
//    val res3 = sql("select empname,empname as b,projectcode from maintable")
    sql("drop datamap if exists datamap1")
    sql("create datamap datamap1 using 'mv' as select empname,empname,projectcode from maintable")
//    val analyzed1 = sql("select empname,empname,projectcode from maintable").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed1, "datamap1"))
//    checkAnswer(sql("select empname,empname,projectcode from maintable"), res1)
//    val analyzed2 = sql("select empname,projectcode from maintable").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed2, "datamap1"))
//    checkAnswer(sql("select empname,empname,projectcode from maintable"), res2)
//    val analyzed3 = sql("select empname,empname as b,projectcode from maintable").queryExecution.analyzed
//    assert(TestUtil.verifyMVDataMap(analyzed3, "datamap1"))
//    checkAnswer(sql("select empname,empname as b,projectcode from maintable"), res3)

    sql("select empname,empname as b,projectcode from maintable").show(false)
  }

}
