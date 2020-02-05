
package org.apache.spark.sql.common.util

import org.apache.carbondata.common.logging.LogServiceFactory
import org.scalatest.{FunSuite, Outcome}


private[spark] abstract class CarbonFunSuite extends FunSuite {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should should mix in
   * the {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      LOGGER.info(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      LOGGER.info(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

}
