# Spark Examples

A few simple [Apache Spark](https://spark.apache.org/) examples demonstrating basic functionality. These examples can certainly be found elsewhere on the web or in the Apache Spark tutorials - however, it is sometimes useful to have examples you have written yourself and understand better. 

Hopefully this section will grow over time.
  
## elt-with-objects
f
*ELT* - Extract Load Transform (a slight variation on the ETL you might be familiar with). Basically show cases how Apache Spark can be used as part of a data pipeline to load and validate CSV data using an object model. 
 
The model for these market objects are driven off a Java definition under "src/main/java/uk/co/devworx/spark_examples/elt/model". It contains the basics - e.g. beans, immutable object builders and validators. 

## Running the Examples

Assuming you have a standard Java and Maven environment set up - you can simply execute the two test cases located under "src/test/java/uk/co/devworx/spark_examples/elt"  

## Running in Spark Shell 

You need to download Apache Spark from the downloads area - [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html). This particular version was tested with Apache Spark 2.4.5.

Once you have extracted the binaries, you can start up the shell with the appropriate dependencies included. In this case, it includes the domain model classes from the built 'elt-with-objects' project:

```
./spark-shell --jars /mnt/JS/git/data-cornucopia/spark-examples/elt-with-objects/target/elt-with-objects-1.0-SNAPSHOT.jar
```

![Alt text](elt-with-objects/src/main/resources/screenshots/01-spark-shell.png?raw=true "Shell Startup")

Now that you have the Spark shell started up, you should be able to run a number of examples :

### 01) Load CSV File Into Stock Objects, Calculate Mid Prices

You should be able to copy and paste these into your Apache Spark shell (assuming you have corrected the CSV path to point to the correct locations on your machine).

The first example reads from the CSV file, converts the rows to objects (using the builder from the object model) and then invokes the individual objects to 

This code is also available in "src/main/scala/01-load-csv-file-calculate-mid.scala".

```scala
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
```

### 02) Load CSV File and Run Validations

The second example reads from the CSV file and runs the validation from the object builder on each row.

This code is also available in "src/main/scala/02-load-csv-file-run-validations.scala".

```scala
import uk.co.devworx.spark_examples.elt.model._
import spark.implicits._
import org.apache.spark.sql._
import java.util._

//Change the file location for your system as appropriate 
val csvLocation = "/mnt/JS/git/data-cornucopia/spark-examples/elt-with-objects/src/main/resources/data/EOD-Stock-Prices-2020-02-12-Broken-Subset.csv"
var df = spark.read.format("csv").option("header", "true").load(csvLocation)

implicit val encoder = org.apache.spark.sql.Encoders.javaSerialization(classOf[List[BuildValidationFailure]])

var df_failures = df.map(row => StockPrice.builder()
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
                                          .getValidationFailures()     
                        )
					        
df_failures.foreach(fef =>  ( if(fef.size() > 0) {  println(fef)  }  )  )
```



