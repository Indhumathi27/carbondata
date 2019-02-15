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

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TestAlterOperationsOnMV extends QueryTest with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop table IF EXISTS testtable")
    sql("create table testtable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table testtable select 'abc',21,2000")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    checkResult
  }

  private def checkResult = {
    checkAnswer(sql("select name,sum(price) from maintable group by name"),
      sql("select name,sum(price) from maintable group by name"))
  }

  override def afterEach(): Unit = {
    sql("drop table IF EXISTS maintable")
    sql("drop table IF EXISTS testtable")
  }

  test("test alter add column on maintable") {
    sql("alter table maintable add columns(d int)")
    sql("insert into table maintable select 'abc',21,2000,30")
    sql("rebuild datamap dm1")
    checkResult
  }

  test("test alter add column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table add columns(d int)")
    }
  }

  test("test drop column on maintable") {
    // check drop column not present in datamap table
    sql("alter table maintable drop columns(c_code)")
    checkResult
    // check drop column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable drop columns(name)")
    }
  }

  test("test alter drop column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table drop columns(dm1_table_name)")
    }
  }

  test("test rename column on maintable") {
    // check rename column not present in datamap table
    sql("alter table maintable change c_code d_code int")
    checkResult
    // check rename column present in mv datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change name name1 string")
    }
  }

  test("test alter rename column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table change sum_price sum_cost int")
    }
  }

  test("test alter rename table") {
    //check rename maintable
    intercept[MalformedCarbonCommandException] {
      sql("alter table maintable rename to maintable_rename")
    }
    //check rename datamaptable
    intercept[MalformedCarbonCommandException] {
      sql("alter table dm1_table rename to dm11_table")
    }
  }

  test("test alter change datatype") {
    //change datatype for column
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change price price bigint")
    }
    //change datatype for column not present in datamap table
    sql("alter table maintable change c_code c_code bigint")
    checkResult
    //change datatype for column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table change sum_price sum_price bigint")
    }
  }
}
