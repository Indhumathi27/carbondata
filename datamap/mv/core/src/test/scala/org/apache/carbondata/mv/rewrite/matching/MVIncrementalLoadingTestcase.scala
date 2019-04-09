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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.spark.exception.ProcessMetaDataException


/**
 * Test Class to verify Incremental Load and unsupported operations for MV Datamap
 */

class MVIncrementalLoadingTestcase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("drop table IF EXISTS test_table")
    sql("drop table IF EXISTS test_table1")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS dimensiontable")
    sql("drop table if exists products")
    sql("drop table if exists sales")
    sql("drop table if exists products1")
    sql("drop table if exists sales1")
  }

  test("test Incremental Loading on rebuild MV Datamap") {
    //create table and load data
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table1")
    //create datamap on table test_table
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    //check sync_info
    assert(DataMapStatusManager.readDataMapStatusMap().get("datamap1").getSegmentInfo
      .get("test_table").toString.equalsIgnoreCase("[]"))
    val query: String = "select empname from test_table"
    val df1 = sql(s"$query")
    val analyzed1 = df1.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed1, "datamap1"))
    sql(s"rebuild datamap datamap1")
    assert(DataMapStatusManager.readDataMapStatusMap().get("datamap1").getSegmentInfo
      .get("test_table").toString.equalsIgnoreCase("[0]"))
    val df2 = sql(s"$query")
    val analyzed2 = df2.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed2, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql(s"rebuild datamap datamap1")
    assert(DataMapStatusManager.readDataMapStatusMap().get("datamap1").getSegmentInfo
      .get("test_table").toString.equalsIgnoreCase("[0, 1]"))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
    val df3 = sql(s"$query")
    val analyzed3 = df3.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed3, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    val df4 = sql(s"$query")
    val analyzed4 = df4.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed4, "datamap1"))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
  }

  test("test MV incremental loading with main table having Marked for Delete segments") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table1")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql("Delete from table test_table where segment.id in (0)")
    sql("Delete from table test_table1 where segment.id in (0)")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql(s"rebuild datamap datamap1")
    assert(DataMapStatusManager.readDataMapStatusMap().get("datamap1").getSegmentInfo
      .get("test_table").toString.equalsIgnoreCase("[1, 2]"))
    DataMapStatusManager.readDataMapStatusMap().get("datamap1").getSegmentInfo
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
  }

  test("test MV incremental loading with multiple parent tables") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS dimensiontable")
    sql(
      """
        | CREATE TABLE main_table
        | (id Int,
        | name String,
        | city String,
        | age Int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      """
        | CREATE TABLE dimensiontable
        | (name String,
        | address String)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    val testData = s"$resourcesPath/sample.csv"
    sql(s"""LOAD DATA  INPATH '$testData' into table main_table""")
    sql(
      s"""insert into dimensiontable select name, concat(city, ' street1') as address from
         |main_table group by name, address""".stripMargin)
    sql("drop datamap if exists simple_agg_with_join")
    sql(
      s"""create datamap simple_agg_with_join using 'mv' as
         | select id,address, sum(age) from main_table inner join dimensiontable on main_table
         | .name=dimensiontable.name group by id ,address""".stripMargin)
    sql(s"rebuild datamap simple_agg_with_join")
    val df = sql(
      s"""select id,address, sum(age) from main_table inner join dimensiontable on main_table
         |.name=dimensiontable.name group by id ,address""".stripMargin)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "simple_agg_with_join"))
  }

  test("test MV incremental loading with delete segment by id on main table") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("Delete from table test_table where segment.id in (0)")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    sql(s"rebuild datamap datamap1")
    intercept[UnsupportedOperationException] {
      sql("Delete from table test_table where segment.id in (1)")
    }
    intercept[UnsupportedOperationException] {
      sql("Delete from table datamap1_table where segment.id in (0)")
    }

  }

  test("test MV incremental loading with delete segment by date on main table") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("Delete from table test_table where segment.id in (0)")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    sql(s"rebuild datamap datamap1")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE test_table WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE datamap1_table WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }
  }

  test("test MV incremental loading with update operation on main table") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
    sql("create table main_table(a string,b string,c int) stored by 'carbondata'")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table testtable(a string,b string,c int) stored by 'carbondata'")
    sql("insert into testtable values('a','abc',1)")
    sql("insert into testtable values('b','bcd',2)")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select a, sum(b) from main_table group by a")
    sql(s"rebuild datamap datamap1")
    var df = sql(
      s"""select a, sum(b) from main_table group by a""".stripMargin)
    var analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("update main_table set(a) = ('aaa') where b = 'abc'").show(false)
    sql("update testtable set(a) = ('aaa') where b = 'abc'").show(false)
    var details = DataMapStatusManager.readDataMapStatusDetails()
    details.foreach(detail => if(detail.getDataMapName.equalsIgnoreCase("datamap1")) {
      assert(detail.getSegmentInfo.get("main_table").isEmpty)
    })
    checkAnswer(sql("select * from main_table"), sql("select * from testtable"))
    sql(s"rebuild datamap datamap1")
    df = sql(
      s""" select a, sum(b) from main_table group by a"""
        .stripMargin)
    analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
  }

  test("test delete operation on main table which has MV Datamap") {
    createTableFactTable("test_table")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    sql(s"rebuild datamap datamap1")
    intercept[UnsupportedOperationException] {
      sql("Delete from test_table where empname = 'arvind'")
    }
  }

  test("test compaction on mv datamap table") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql(s"rebuild datamap datamap1")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    checkExistence(sql("show segments for table datamap1_table"),false, "0.1")
    sql("alter datamap datamap1 compact 'major'")
    checkExistence(sql("show segments for table datamap1_table"),true, "0.1")
    sql("clean files for table datamap1_table")
  }

  test("test auto-compaction on mv datamap table") {
    sql("set carbon.enable.auto.load.merge=true")
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    checkExistence(sql("show segments for table datamap1_table"),false, "0.1")
    loadDataToFactTable("test_table")
    sql(s"rebuild datamap datamap1")
    checkExistence(sql("show segments for table datamap1_table"),true, "0.1")
    sql("clean files for table datamap1_table")
  }

  test("test insert overwrite") {
    sql("drop table IF EXISTS mt")
    sql("create table mt(a string,b string,c int) stored by 'carbondata'")
    sql("insert into mt values('a','abc',1)")
    sql("insert into mt values('b','bcd',2)")
    sql("drop datamap if exists datamap1")
    sql(
      "create datamap datamap1 using 'mv' as select a, sum(b) from mt  group by a")
    sql(s"rebuild datamap datamap1")
    sql(" select a, sum(b) from mt  group by a").show(false)
    sql("insert overwrite table mt select 'd','abc',3")
    var details = DataMapStatusManager.readDataMapStatusDetails()
    details.foreach(detail => if(detail.getDataMapName.equalsIgnoreCase("datamap1")) {
      assert(detail.getSegmentInfo.get("mt").isEmpty)
    })
    sql(" select a, sum(b) from mt  group by a").show(false)
    sql(s"rebuild datamap datamap1")
    details = DataMapStatusManager.readDataMapStatusDetails()
    details.foreach(detail => if(detail.getDataMapName.equalsIgnoreCase("datamap1")) {
      assert(detail.getSegmentInfo.get("mt").contains("2"))
    })
    sql("drop table IF EXISTS mt")
  }


  test("test inner join with mv") {
    sql("drop table if exists products")
    sql("create table products (product string, amount int) stored by 'carbondata' ")
    sql("load data INPATH '/home/root1/Desktop/products.csv' into table products")
    sql("drop table if exists sales")
    sql("create table sales (product string, quantity int) stored by 'carbondata'")
    sql("load data INPATH '/home/root1/Desktop/sales.csv' into table sales")
    sql("drop datamap if exists innerjoin")
    sql("Create datamap innerjoin using 'mv' as Select p.product, p.amount, s.quantity from " +
        "products p, sales s where p.product=s.product")
    sql("drop table if exists products1")
    sql("create table products1 (product string, amount int) stored by 'carbondata' ")
    sql("load data INPATH '/home/root1/Desktop/products.csv' into table products1")
    sql("drop table if exists sales1")
    sql("create table sales1 (product string, quantity int) stored by 'carbondata'")
    sql("load data INPATH '/home/root1/Desktop/sales.csv' into table sales1")
    sql(s"rebuild datamap innerjoin")
    checkAnswer( sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product") )
    sql("insert into products values('Biscuits',10)")
    sql("insert into products1 values('Biscuits',10)")
    sql(s"rebuild datamap innerjoin")
    checkAnswer( sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product") )
    sql("insert into sales values('Biscuits',100)")
    sql("insert into sales1 values('Biscuits',100)")
    checkAnswer( sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product") )
  }

  test("test block drop partition for tables having mv") {
    sql("drop table if exists par_table")
    sql("CREATE TABLE par_table(id INT, name STRING, age INT) PARTITIONED BY(city STRING) STORED BY 'carbondata'")
    sql(s"LOAD DATA INPATH '$resourcesPath/sample.csv' into table par_table")
    sql("drop datamap if exists p1")
    sql("create datamap p1 on table par_table using 'mv' as select city, id from par_table")
    sql("rebuild datamap p1")
    intercept[ProcessMetaDataException] {
      sql("alter table par_table drop partition (city='shenzhen')")
    }
  }

  test("test set segments with main table having mv datamap") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS test_table")
    sql("create table main_table(a string,b string,c int) stored by 'carbondata'")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table test_table(a string,b string,c int) stored by 'carbondata'")
    sql("insert into test_table values('a','abc',1)")
    sql("insert into test_table values('b','bcd',2)")
    sql("drop datamap if exists datamap_mt")
    sql(
      "create datamap datamap_mt using 'mv' as select a, sum(b) from main_table  group by a")
    sql(s"rebuild datamap datamap_mt")
    checkAnswer(sql("select a, sum(b) from main_table  group by a"),
      sql("select a, sum(b) from test_table  group by a"))
    sql("SET carbon.input.segments.default.main_table = 1")
    sql("SET carbon.input.segments.default.test_table=1")
    checkAnswer(sql("select a, sum(b) from main_table  group by a"),
      sql("select a, sum(b) from test_table  group by a"))
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS test_table")
  }


  test("test set segments with main table having mv datamap before rebuild") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) stored by 'carbondata'")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("drop datamap if exists datamap")
    sql(
      "create datamap datamap using 'mv' as select a, sum(b) from main_table  group by a")
    sql("SET carbon.input.segments.default.main_table=1")
    sql(s"rebuild datamap datamap")
    sql("select a, sum(b) from main_table  group by a").show(false)
    sql("select * from datamap_table").show(false)
    sql("reset")
    sql("select a, sum(b) from main_table  group by a").show(false)
  }

  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName + "_table"))
  }

  override def afterAll(): Unit = {
    sql("drop table if exists products")
    sql("drop table if exists sales")
    sql("drop table if exists products1")
    sql("drop table if exists sales1")
    sql("drop table IF EXISTS test_table")
    sql("drop table IF EXISTS test_table1")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS dimensiontable")
  }

  private def createTableFactTable(tableName: String) = {
    sql(s"drop table IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (empname String, designation String, doj Timestamp,
         |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
         |  utilization int,salary int)
         | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  private def loadDataToFactTable(tableName: String) = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE $tableName  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }
}
