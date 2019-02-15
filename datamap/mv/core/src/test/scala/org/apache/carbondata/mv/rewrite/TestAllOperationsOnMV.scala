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

class TestAllOperationsOnMV extends QueryTest with BeforeAndAfterEach {

//    override def beforeEach(): Unit = {
//      sql("drop table IF EXISTS maintable")
//      sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
//      sql("insert into table maintable select 'abc',21,2000")
//      sql("drop table IF EXISTS testtable")
//      sql("create table testtable(name string, c_code int, price int) stored by 'carbondata'")
//      sql("insert into table testtable select 'abc',21,2000")
//      sql("drop datamap if exists dm1")
//      sql("create datamap dm1 on table maintable using 'mv' as select name,sum(price) " +
//          "from maintable group by name")
//      sql("rebuild datamap dm1")
//      checkResult()
//    }

  private def checkResult():Unit = {
    checkAnswer(sql("select name,sum(price) from maintable group by name"),
      sql("select name,sum(price) from maintable group by name"))
  }

//    override def afterEach(): Unit = {
//      sql("drop table IF EXISTS maintable")
//      sql("drop table IF EXISTS testtable")
//    }

  test("test alter add column on maintable") {
    sql("alter table maintable add columns(d int)")
    sql("insert into table maintable select 'abc',21,2000,30")
    sql("rebuild datamap dm1")
    checkResult()
  }

  test("test alter add column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table add columns(d int)")
    }
  }

  test("test drop column on maintable") {
    // check drop column not present in datamap table
    sql("alter table maintable drop columns(c_code)")
    checkResult()
    // check drop column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable drop columns(name)")
    }
    sql("drop datamap if exists dm1")
  }

  test("test alter drop column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table drop columns(dm1_table_name)")
    }
  }

  test("test rename column on maintable") {
    // check rename column not present in datamap table
    sql("alter table maintable change c_code d_code int")
    checkResult()
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
    checkResult()
    //change datatype for column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table change sum_price sum_price bigint")
    }
  }

  test("test dmproperties") {
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' dmproperties" +
        "('LOCAL_DICTIONARY_ENABLE'='false') as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Local Dictionary Enabled false")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' dmproperties('TABLE_BLOCKSIZE'='256 MB') " +
        "as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Table Block Size  256 MB")
  }

  test("test table properties") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('LOCAL_DICTIONARY_ENABLE'='false')")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Local Dictionary Enabled false")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('TABLE_BLOCKSIZE'='256 MB')")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Table Block Size  256 MB")
  }

  test("test delete segment by id on main table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("Delete from table maintable where segment.id in (1)")
    }
    intercept[UnsupportedOperationException] {
      sql("Delete from table dm1_table where segment.id in (0)")
    }
  }

  test("test delete segment by date on main table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE maintable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE dm1_table WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }
  }

  test("test delete operation on main table which has MV Datamap") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("Delete from maintable where name = 'abc'")
    }
  }

  test("test partition table with mv") {
    sql("drop table if exists par_table")
//    sql("CREATE TABLE par_table(id INT, name STRING, age INT) PARTITIONED BY(city string, addr string) STORED BY 'carbondata'")
    sql("CREATE TABLE par_table(id INT, name STRING, age INT, city string, addr string) STORED BY 'carbondata'")

    sql("insert into par_table values(1,'abc',3,'def', 'bang')")
    sql("drop datamap if exists p1")
    sql("create datamap p1 on table par_table using 'mv' as select city,id,addr from par_table")
    sql("rebuild datamap p1")
    sql("select * from p1_table").show(false)
    sql("select addr,id,city from par_table").show( false)
//    sql("explain extended select city,id from par_table").show(100, false)

//    sql("drop table if exists products")
//    sql("create table products (product string) partitioned by (amount int) stored by 'carbondata' ")
//    sql("load data INPATH '/home/root1/Desktop/products.csv' into table products")
//    sql("drop table if exists sales")
//    sql("create table sales (product string) partitioned by (quantity int) stored by 'carbondata'")
//    sql("load data INPATH '/home/root1/Desktop/sales.csv' into table sales")
//    sql("drop datamap if exists innerjoin")
//    sql("Create datamap innerjoin using 'mv' as Select p.product, p.amount, s.quantity, s.product from " +
//        "products p, sales s where p.product=s.product")
//        sql("rebuild datamap innerjoin")
//    sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product").show(false)

  }

  test("join on three tables") {
    sql("create table student(s_id int, s_name string) STORED BY 'carbondata'")
    sql("insert into student values(1, 'Jack')")
    sql("create table marks(school_id int, s_id int, score int, status string) STORED BY 'carbondata'")
    sql("insert into marks values(1004, 1, 23, 'fail')")
    sql("create table details(address_city string, email_ID string,school_id int, accomplishments string) STORED BY 'carbondata'")
    sql("insert into details values('Banglore',  'jsingh@geeks.com', 1020, 'ACM ICPC selected')")
    sql("drop datamap if exists p1")
    sql("create datamap p1  using 'mv' as select s_name,  " +
        "address_city,accomplishments from student s inner join marks m on s.s_id = m.s_id inner join details d on d.school_id = m.school_id")
  }

}

