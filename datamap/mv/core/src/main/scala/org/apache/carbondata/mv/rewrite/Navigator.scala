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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, Literal, NamedExpression, ScalaUDF}
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesFunction
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.metadata.schema.datamap.Granularity
import org.apache.carbondata.mv.datamap.MVUtil
import org.apache.carbondata.mv.expressions.modular._
import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular._
import org.apache.carbondata.mv.session.MVSession

private[mv] class Navigator(catalog: SummaryDatasetCatalog, session: MVSession) {

  def rewriteWithSummaryDatasets(plan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    val replaced = plan.transformAllExpressions {
      case s: ModularSubquery =>
        if (s.children.isEmpty) {
          rewriteWithSummaryDatasetsCore(s.plan, rewrite) match {
            case Some(rewrittenPlan) => ScalarModularSubquery(rewrittenPlan, s.children, s.exprId)
            case None => s
          }
        }
        else throw new UnsupportedOperationException(s"Rewrite expression $s isn't supported")
      case o => o
    }
    rewriteWithSummaryDatasetsCore(replaced, rewrite).getOrElse(replaced)
  }

  def rewriteWithSummaryDatasetsCore(plan: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    val rewrittenPlan = plan transformDown {
      case currentFragment =>
        if (currentFragment.rewritten || !currentFragment.isSPJGH) currentFragment
        else {
          val compensation =
            (for { dataset <- catalog.lookupFeasibleSummaryDatasets(currentFragment).toStream
                   subsumer <- session.sessionState.modularizer.modularize(
                     session.sessionState.optimizer.execute(dataset.plan)).map(_.semiHarmonized)
                   subsumee <- unifySubsumee(currentFragment)
                   comp <- subsume(
                     unifySubsumer2(
                       unifySubsumer1(
                         subsumer,
                         subsumee,
                         dataset.relation),
                       subsumee),
                     subsumee, rewrite)
                 } yield comp).headOption
          compensation.map(_.setRewritten).getOrElse(currentFragment)
        }
    }
    if (rewrittenPlan.fastEquals(plan)) {
      val timeSeriesUdf = rewrittenPlan match {
        case s: Select =>
          getTimeseriesUdf(s.outputList)
        case g: GroupBy =>
          getTimeseriesUdf(g.outputList)
      }
      if (null != timeSeriesUdf._2) {
        // check for rollup and rewrite the plan
        val datasets = catalog.lookupFeasibleSummaryDatasets(rewrittenPlan)
        val rewrittenQuery: java.lang.String = rewrittenPlan.asCompactSQL
        var queryGranularity: String = null
        var udfName: String = null
        datasets.foreach { dataset =>
          //
          val datamapPlan = session.sessionState.modularizer.modularize(
            session.sessionState.optimizer.execute(dataset.plan)).next().harmonized
          val datamapQuery = datamapPlan match {
            case s: Select =>
              getTimeseriesUdf(s.outputList)
            case g: GroupBy =>
              getTimeseriesUdf(g.outputList)
          }
          if (Granularity.valueOf(timeSeriesUdf._2.toUpperCase).ordinal() <=
            Granularity.valueOf(datamapQuery._2.toUpperCase).ordinal()) {
            udfName = datamapQuery._1
            if (rewrittenQuery.replace(timeSeriesUdf._1, datamapQuery._1)
              .equalsIgnoreCase(datamapPlan.asCompactSQL)) {
              if (queryGranularity == null) {
                queryGranularity = datamapQuery._2
              } else if (Granularity.valueOf(queryGranularity.toUpperCase()).ordinal() >=
                         Granularity.valueOf(datamapQuery._2.toUpperCase).ordinal()) {
                queryGranularity = datamapQuery._2
              }
            }
          }
        }
        if(null != queryGranularity) {
          val newPlan = plan.transformDown {
            case p => p.transformAllExpressions {
              case tudf: ScalaUDF =>
                if (tudf.function.isInstanceOf[TimeSeriesFunction]) {
                  var children: Seq[Expression] = Seq.empty
                  tudf.collect {
                    case attr: AttributeReference =>
                      children = children :+ attr
                      attr
                  }
                  children = children :+
                             new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                               DataTypes.StringType)
                  ScalaUDF(tudf.function,
                    tudf.dataType,
                    children,
                    tudf.inputTypes,
                    tudf.udfName,
                    tudf.nullable,
                    tudf.deterministic)
                } else {
                  tudf
                }
              case a@Alias(tudf: ScalaUDF, _) =>
                if (tudf.function.isInstanceOf[TimeSeriesFunction]) {
                  var children: Seq[Expression] = Seq.empty
                  tudf.collect {
                    case attr: AttributeReference =>
                      children = children :+ attr
                      attr
                  }
                  children = children :+
                             new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                               DataTypes.StringType)
                  Alias(ScalaUDF(tudf.function,
                    tudf.dataType,
                    children,
                    tudf.inputTypes,
                    tudf.udfName,
                    tudf.nullable,
                    tudf.deterministic), udfName)(a.exprId,
                    a.qualifier).asInstanceOf[NamedExpression]
                } else {
                  a
                }
            }
          }
          val modifiedPlan = rewriteWithSummaryDatasetsCore(newPlan, rewrite)
          modifiedPlan.get.map(_.setRolledUp())
          modifiedPlan
        } else {
          None
        }
      } else {
        None
      }
    } else {
      Some(rewrittenPlan)
    }
  }

    def getTimeseriesUdf(outputList: scala.Seq[NamedExpression]): (String, String) = {
      outputList.collect {
        case Alias(udf: ScalaUDF, name) =>
          if (udf.function.isInstanceOf[TimeSeriesFunction]) {
            udf.children.collect {
              case l: Literal =>
                return (name, l.value.toString)
            }
          }
      }
      (null, null)
    }

  def subsume(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    if (subsumer.getClass == subsumee.getClass) {
      (subsumer.children, subsumee.children) match {
        case (Nil, Nil) => None
        case (r, e) if r.forall(_.isInstanceOf[modular.LeafNode]) &&
                       e.forall(_.isInstanceOf[modular.LeafNode]) =>
          val iter = session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
          if (iter.hasNext) Some(iter.next)
          else None

        case (rchild :: Nil, echild :: Nil) =>
          val compensation = subsume(rchild, echild, rewrite)
          val oiter = compensation.map {
            case comp if comp.eq(rchild) =>
              session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
            case _ =>
              session.sessionState.matcher.execute(subsumer, subsumee, compensation, rewrite)
          }
          oiter.flatMap { case iter if iter.hasNext => Some(iter.next)
                          case _ => None }

        case _ => None
      }
    } else {
      None
    }
  }

  // add Select operator as placeholder on top of subsumee to facilitate matching
  def unifySubsumee(subsumee: ModularPlan): Option[ModularPlan] = {
    subsumee match {
      case gb @ modular.GroupBy(_, _, _, _,
        modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _) =>
        Some(
          Select(gb.outputList, gb.outputList, Nil, Map.empty, Nil, gb :: Nil, gb.flags,
            gb.flagSpec, Seq.empty))
      case other => Some(other)
    }
  }

  // add Select operator as placeholder on top of subsumer to facilitate matching
  def unifySubsumer1(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      dataMapRelation: ModularPlan): ModularPlan = {
    // Update datamap table relation to the subsumer modular plan
    val mVUtil = new MVUtil
    val updatedSubsumer = subsumer match {
      // In case of order by it adds extra select but that can be ignored while doing selection.
      case s@Select(_, _, _, _, _, Seq(g: GroupBy), _, _, _, _) =>
        s.copy(children = Seq(g.copy(dataMapTableRelation = Some(dataMapRelation))),
          outputList = mVUtil.updateDuplicateColumns(s.outputList))
      case s: Select => s
        .copy(dataMapTableRelation = Some(dataMapRelation),
          outputList = mVUtil.updateDuplicateColumns(s.outputList))
      case g: GroupBy => g
        .copy(dataMapTableRelation = Some(dataMapRelation),
          outputList = mVUtil.updateDuplicateColumns(g.outputList))
      case other => other
    }
    (updatedSubsumer, subsumee) match {
      case (r @
        modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _),
        modular.Select(_, _, _, _, _,
          Seq(modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)),
          _, _, _, _)
        ) =>
        modular.Select(
          r.outputList, r.outputList, Nil, Map.empty, Nil, r :: Nil, r.flags,
          r.flagSpec, Seq.empty).setSkip()
      case _ => updatedSubsumer.setSkip()
    }
  }

  def unifySubsumer2(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    val rtables = subsumer.collect { case n: modular.LeafNode => n }
    val etables = subsumee.collect { case n: modular.LeafNode => n }
    val pairs = for {
      i <- rtables.indices
      j <- etables.indices
      if rtables(i) == etables(j) && reTablesJoinMatched(
        rtables(i), etables(j), subsumer, subsumee, i, j
      )
    } yield (rtables(i), etables(j))

    pairs.foldLeft(subsumer) {
      case (curSubsumer, pair) =>
        val mappedOperator =
          pair._1 match {
            case relation: HarmonizedRelation if relation.hasTag =>
              pair._2.asInstanceOf[HarmonizedRelation].addTag
            case _ =>
              pair._2
          }
        val nxtSubsumer = curSubsumer.transform {
          case node: ModularRelation if node.fineEquals(pair._1) => mappedOperator
          case pair._1 if !pair._1.isInstanceOf[ModularRelation] => mappedOperator
        }

        // val attributeSet = AttributeSet(pair._1.output)
        // reverse first due to possible tag for left join
        val rewrites = AttributeMap(pair._1.output.zip(mappedOperator.output))
        nxtSubsumer.transformUp {
          case p => p.transformExpressions {
            case a: Attribute if rewrites contains a => rewrites(a).withQualifier(a.qualifier)
          }
        }
    }
  }

  // match the join table of subsumer and subsumee
  // when the table names are the same
  def reTablesJoinMatched(rtable: modular.LeafNode, etable: modular.LeafNode,
                          subsumer: ModularPlan, subsumee: ModularPlan,
                          rIndex: Int, eIndex: Int): Boolean = {
    (rtable, etable) match {
      case _: (ModularRelation, ModularRelation) =>
        val rtableParent = subsumer.find(p => p.children.contains(rtable)).get
        val etableParent = subsumee.find(p => p.children.contains(etable)).get
        (rtableParent, etableParent) match {
          case  (e: Select, r: Select) =>
            val intersetJoinEdges = r.joinEdges intersect e.joinEdges
            if (intersetJoinEdges.nonEmpty) {
              return intersetJoinEdges.exists(j => j.left == rIndex && j.left == eIndex ||
                j.right == rIndex && j.right == eIndex)
            }
          case _ => false
        }
    }
    true
  }
}
