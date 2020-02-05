
package org.apache.spark.sql.common.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._

/**
 * Provides helper methods for comparing plans.
 */
class PlanTest extends CarbonFunSuite {

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
    }
  }
}
