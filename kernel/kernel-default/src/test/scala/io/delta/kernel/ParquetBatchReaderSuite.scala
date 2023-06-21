package io.delta.kernel

import java.io.File
import java.net.URI
import java.nio.file.Path

import io.delta.kernel.parquet.ParquetBatchReader
import io.delta.kernel.data.Row
import io.delta.kernel.types.{LongType, StructField, StructType}
import io.delta.kernel.util.GoldenTableUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class ParquetBatchReaderSuite extends AnyFunSuite with GoldenTableUtils {

  private def getRowsFromFile(
    reader: ParquetBatchReader,
    fileName: String,
    schema: StructType): Seq[Row] = {
    var result = Seq.empty[Row]
    val iter = reader.read(fileName, schema)
    try {
      while (iter.hasNext) {
        val batch = iter.next()
        val rowIter = batch.getRows
        try {
          while (rowIter.hasNext) {
            result = result :+ rowIter.next
          }
        } finally {
          rowIter.close()
        }
      }
    } finally {
      iter.close()
    }
    result
  }

  test("don't request row-indexes") {
    withGoldenTable("parquet-basic-row-indexes") { path =>
      val dir = new File(new URI(path))
      val parquetFiles = dir.listFiles()
        .filter { file =>
          file.getName().endsWith(".parquet")
        }

      val readSchema = new StructType()
        .add("id", LongType.INSTANCE)
      val parquetBatchReader = new ParquetBatchReader(new Configuration())

      // there should be three files [0, 10), [10, 20), [20, 30)
      parquetFiles.map { file =>
        val rows = getRowsFromFile(parquetBatchReader, file.getAbsolutePath, readSchema)
        rows.map(_.getLong(0))
      }.flatten.toSet == Set(Range(0, 30))
    }
  }

  test("request row indexes") {
    withGoldenTable("parquet-basic-row-indexes") { path =>
      val dir = new File(new URI(path))
      val parquetFiles = dir.listFiles()
        .filter { file =>
          file.getName().endsWith(".parquet")
        }

      val readSchema = new StructType()
        .add("id", LongType.INSTANCE)
        .add(StructField.ROW_INDEX_COLUMN)
      val parquetBatchReader = new ParquetBatchReader(new Configuration())

      parquetFiles.foreach { file =>
        val rows = getRowsFromFile(parquetBatchReader, file.getAbsolutePath, readSchema)
        // row index should = id % 10
        rows.foreach { row =>
          assert(row.getLong(0) % 10 == row.getLong(1))
        }
      }
    }
  }
}