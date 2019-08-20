/*
 * Copyright 2018-2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.{Dataset, Row}
import za.co.absa.atum.core.Constants

/**
  * This object contains utils for traversing execution plan DAG to infer control measurement input/output paths
  */
object ExecutionPlanUtils {

  private val log = LogManager.getLogger("Atum.ExecutionPlanUtils")

  /**
    * The method returns input control measurements info file name inferred from the source dataset
    * Ensure one and only one input pathname has input control measurements
    *
    * @param dataset A dataset where input path name(s) will be searched
    * @param infoFileName A file name of an info file, e.g. "_INFO"
    *
    * @return The inferred input control measurements file path of the source dataset
    */
  def inferInputInfoFileName(dataset: Dataset[Row], infoFileName: String = Constants.DefaultInfoFileName): Path = {
    val plan = dataset.queryExecution.logical
    val paths = getSourceFileNames(plan)
    if (paths.isEmpty) {
      throw new IllegalStateException("Control framework was unable to infer dataset input file name.")
    }
    val hadoopConfiguration = dataset.sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfiguration)
    val infoNames = paths.flatMap(p => {
      val infoName = new Path(p, infoFileName)
      log.info(s"Inferred info file name: $infoName, from path $p and name $infoFileName")
      Some(infoName).filter(fs.exists)
    })
    val path = infoNames match {
      case List(p) => p
      case _ :: _ => throw new IllegalStateException("Ambiguous control measurements file names: " + infoNames
        .mkString(","))
      case _ => throw new IllegalStateException("Control framework was unable to infer dataset input file name.")
    }
    path
  }

  def getHadoopFullPath(path: Path, hadoopConfiguration: Configuration): Path = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.getFileStatus(path).getPath
  }

  /**
    * The method returns output file name inferred from the source dataset
    *
    * @param qe A query execution object where output path name will be searched
    *
    * @return The inferred output control measurements file path of the source dataset
    */
  def inferOutputFileName(qe: QueryExecution, hadoopConfiguration: Configuration): Option[Path] = {
    qe.analyzed match {
      case s: SaveIntoDataSourceCommand =>
        Some(getHadoopFullPath(new Path(s.options("path")), hadoopConfiguration))
      case h: InsertIntoHadoopFsRelationCommand =>
        Some(h.outputPath)
      case a =>
        log.warn(s"Logical plan: ${qe.logical.treeString}")
        log.warn(s"Analyzed plan: ${qe.analyzed.treeString}")
        log.warn(s"Optimized plan: ${qe.optimizedPlan.treeString}")
        log.error(s"Unable to infer storage path to output control measurements to for query execution $qe.")
        None
    }
  }

  /**
    * The method returns output control measurements info file name inferred from the source dataset
    *
    * @param qe A query execution object where output path name will be searched
    * @param infoFileName A file name of an info file, e.g. "_INFO"
    *
    * @return The inferred output control measurements file path of the source dataset
    */
  def inferOutputInfoFileName(qe: QueryExecution, infoFileName: String = Constants.DefaultInfoFileName): Option[Path] = {
    qe.analyzed match {
      case s: SaveIntoDataSourceCommand =>
          Some(new Path(s.options("path"), infoFileName))
      case h: InsertIntoHadoopFsRelationCommand =>
          Some(new Path(h.outputPath, infoFileName))
      case a =>
        log.warn(s"Logical plan: ${qe.logical.treeString}")
        log.warn(s"Analyzed plan: ${qe.analyzed.treeString}")
        log.warn(s"Optimized plan: ${qe.optimizedPlan.treeString}")
        log.error(s"Unable to infer output path for control measurements for query execution $qe.")
        None
    }
  }

    /**
    * The method returns source file names of a DataSet execution plan by traversing the DAG.
    * Thanks za.co.absa.spline.core
    *
    * @param plan A logical plan of execution
    *
    * @return The list of input files paths
    */
  def getSourceFileNames(plan: LogicalPlan): List[Path] = {
    plan match {
      case n: LeafNode => n match {
        case a: LogicalRelation =>
          // LogicalRelation is a data source node
          a.relation match {
            case hfsr: HadoopFsRelation => hfsr.location.rootPaths.toList
            case _ => Nil
          }
        case _ => Nil
      }
      case n: UnaryNode => getSourceFileNames(n.child)
      case b: BinaryNode => getSourceFileNames(b.left) ::: getSourceFileNames(b.right)
    }
  }
}
