
import uk.co.devworx.spark_examples.elt.model._
import spark.implicits._
import org.apache.spark.sql._

val csvLocation = "/mnt/JS/git/data-cornucopia/spark-examples/elt-with-objects/src/main/resources/data/EOD-Stock-Prices-2020-02-12.csv"
var df = spark.read.format("csv").option("header", "true").load(csvLocation)

implicit val encoder = org.apache.spark.sql.Encoders.bean(classOf[StockPrice])

var df_stockPrices = df.map(row => StockPrice.builder()
                                             .setStockId(row.getAs(1))
                                             .setPriceDate(row.getAs(2))
                                             .setLastOrMidPrice(row.getAs(5))
                                             .setOpen(row.getAs(6))
                                             .setHigh(row.getAs(7))
                                             .setLow(row.getAs(8))
                                             .setClose(row.getAs(9))
                                             .setVolume(row.getAs(10))
                                             .setAdjustedOpen(row.getAs(11))
                                             .setAdjustedHigh(row.getAs(12))
                                             .setAdjustedLow(row.getAs(13))
                                             .setAdjustedClose(row.getAs(14))
                                             .setAdjustedVolume(row.getAs(15))
                                             .setDividend(row.getAs(16))
                                             .build()     
					        )
					        
df_stockPrices.foreach(fef => println(fef.getStockId() + " -> calculatedMid : " + fef.getCalculatedMid() + ", calculatedAdjustedMid : " + fef.getCalculatedAdjustedMid())) 
				
