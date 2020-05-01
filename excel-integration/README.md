# Excel Intregation

There are still a fair amount of companies out there that have a heavy reliance on the use of desktop Excel - in many cases a rather old version of excel. Yes, Financial Services - that basically means you.

This project contains a simple reference point that can convert from Excel (as was required on an ad-hoc basis by a number of customer projects.)

## Running the Example

The example uses the financial sample from Microsoft -  https://docs.microsoft.com/en-us/power-bi/sample-financial-download

Once built (using mvn) - issue the following from this directory:

```
java -cp "target/appassembler/repo/*" \
uk.co.devworx.excel.ExcelToCSVConverter \
src/test/resources/FinancialSample.xlsx \
Sheet1 \
target/FinancialSample.csv
```

This will create a CSV file from the 'FinancialSample.xlsx' Excel file in the target directory.
 
 

