/**
 * Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Histogram.AggregateFunction
import com.amazon.deequ.analyzers.Histogram.Count
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import com.amazon.deequ.metrics.BinData
import com.amazon.deequ.metrics.DistributionBinned
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.metrics.HistogramBinnedMetric
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType

import scala.util.Failure
import scala.util.Try

/**
 * Histogram analyzer for numerical data with binning support.
 * Currently supports equal-width bins.
 */
case class HistogramBinned(
                            override val column: String,
                            binCount: Option[Int] = None,
                            customEdges: Option[Array[Double]] = None, // TODO: implement
                            override val where: Option[String] = None,
                            override val computeFrequenciesAsRatio: Boolean = true,
                            override val aggregateFunction: AggregateFunction = Count)
  extends HistogramBase(column, where, computeFrequenciesAsRatio, aggregateFunction)
    with Analyzer[FrequenciesAndNumRows, HistogramBinnedMetric] {

  require(binCount.isDefined ^ customEdges.isDefined,
    "Must specify either binCount (equal-width) or customEdges (custom)")

  private var storedEdges: Array[Double] = _

  override def computeStateFrom(data: DataFrame,
                                filterCondition: Option[String] = None): Option[FrequenciesAndNumRows] = {

    val totalCount = if (computeFrequenciesAsRatio) {
      aggregateFunction.total(data)
    } else {
      1
    }

    val filteredData = data.transform(filterOptional(where))

    // Get column type and cast appropriately
    val columnType = filteredData.schema(column).dataType
    val numericCol = columnType match {
      case _: IntegerType => col(column).cast(IntegerType)
      case _: LongType => col(column).cast(LongType)
      case _: FloatType => col(column).cast(FloatType)
      case _: DoubleType => col(column).cast(DoubleType)
      case _ => col(column).cast(DoubleType)
    }

    // TODO: Implement custom edges support
    if (customEdges.isDefined) {
      throw new UnsupportedOperationException("Custom edges not yet implemented")
    }

    // Equal-width mode - calculate edges from data
    val stats = filteredData.select(numericCol).agg(
      org.apache.spark.sql.functions.min(column).alias("min_val"),
      org.apache.spark.sql.functions.max(column).alias("max_val")
    ).collect()(0)

    val minVal = stats.getAs[Number]("min_val").doubleValue()
    val maxVal = stats.getAs[Number]("max_val").doubleValue()

    if (minVal == null || maxVal == null) {
      return Some(FrequenciesAndNumRows(
        data.sparkSession.emptyDataFrame,
        totalCount
      ))
    }

    val binWidth = (maxVal - minVal) / binCount.get
    storedEdges = Array.tabulate(binCount.get + 1)(i => minVal + i * binWidth)

    // Create binning UDF that returns integer bin index
    val binningUdf = udf((value: Number) => {
      if (value == null) {
        -1 // special index for nulls
      } else {
        val binIndex = math.min(((value.doubleValue() - minVal) / binWidth).toInt, binCount.get - 1)
        binIndex
      }
    })

    val binnedData = filteredData.withColumn(column, binningUdf(numericCol))
    val frequencies = aggregateFunction.query(column, binnedData)

    Some(FrequenciesAndNumRows(frequencies, totalCount))
  }

  override def computeMetricFrom(state: Option[FrequenciesAndNumRows]): HistogramBinnedMetric = {
    state match {
      case Some(theState) =>
        val value: Try[DistributionBinned] = Try {
          val countColumnName = theState.frequencies.schema.fields
            .find(field => field.dataType == LongType && field.name != column)
            .map(_.name)
            .getOrElse(throw new IllegalStateException(s"Count column not found in the frequencies DataFrame"))

          val binCount = theState.frequencies.count()

          val histogramDetails = theState.frequencies.collect()
            .map { row =>
              val binValue = row.getAs[String](column)
              val absolute = row.getAs[Long](countColumnName)
              val ratio = absolute.toDouble / theState.numRows
              binValue -> DistributionValue(absolute, ratio)
            }.toMap

          // Convert to BinData objects
          val binDataSeq = (0 until storedEdges.length - 1).map { binIndex =>
            val binStart = storedEdges(binIndex)
            val binEnd = storedEdges(binIndex + 1)
            val distValue = histogramDetails.get(binIndex.toString).getOrElse(DistributionValue(0, 0.0))
            BinData(binStart, binEnd, distValue.absolute, distValue.ratio)
          }.toVector

          DistributionBinned(binDataSeq, binCount)
        }

        HistogramBinnedMetric(column, value)

      case None =>
        HistogramBinnedMetric(column, Failure(Analyzers.emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): HistogramBinnedMetric = {
    HistogramBinnedMetric(column, Failure(MetricCalculationException.wrapIfNecessary(exception)))
  }

  override def preconditions: Seq[StructType => Unit] = {
    Preconditions.hasColumn(column) :: Nil
  }
}
