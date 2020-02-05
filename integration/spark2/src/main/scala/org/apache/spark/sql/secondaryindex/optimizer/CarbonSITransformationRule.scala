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
package org.apache.spark.sql.secondaryindex.optimizer

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 * Rule for rewriting plan if query has a filter on index table column
 */
class CarbonSITransformationRule(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  val secondaryIndexOptimizer: CarbonSecondaryIndexOptimizer =
    new CarbonSecondaryIndexOptimizer(sparkSession)

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (checkIfRuleNeedToBeApplied(plan)) {
      secondaryIndexOptimizer.transformFilterToJoin(plan, isProjectionNeeded(plan))
    } else {
      plan
    }
  }

  private def checkIfRuleNeedToBeApplied(plan: LogicalPlan): Boolean = {
    val relations = CarbonUtils.collectCarbonRelation(plan)
    if (relations.nonEmpty) {
      true
    } else {
      LOGGER.info("skip CarbonOptimizer")
      false
    }
  }

  /**
   * Method to check whether the plan is for create/insert non carbon table(hive, parquet etc).
   * In this case, transformed plan need to add the extra projection, as positionId and
   * positionReference columns will also be added to the output of the plan irrespective of
   * whether the query has requested these columns or not
   *
   * @param plan
   * @return
   */
  private def isProjectionNeeded(plan: LogicalPlan): Boolean = {
    var needProjection = false
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      plan collect {
        case create: CreateHiveTableAsSelectCommand =>
          needProjection = true
        case create: LogicalPlan if (create.getClass.getSimpleName
          .equals("OptimizedCreateHiveTableAsSelectCommand")) =>
          needProjection = true
        case insert: InsertIntoHadoopFsRelationCommand =>
          if (!insert.fileFormat.toString.equals("carbon")) {
            needProjection = true
          }
      }
    }
    needProjection
  }
}
