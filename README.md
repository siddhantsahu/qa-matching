## Setup Guide

### Load stacksample data onto a local SQLite Database
Use `scripts/csv_to_sql.py` to load all 4 csv files - questions, answers, tags and postlinks into tables of the *same name*. to a local SQLite database, say `stack.db`. Store this in your data folder.

Example run command: `python csv_to_sql.py --date_column CreationDate ../data/stack.db ../data/working/stacksample/Questions.csv questions`

For datasets that do not have a date column (or if we don't need parsing date column), drop the optional argument `--date_column`.

### Run Spark ETL job
- Download [SQLite JDBC jar file](https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.23.1.jar)
- Run ETL pipeline job as follows:
```
./bin/spark-submit
    --class ETLPipeline
    --jars pathToSqliteJar
    --conf spark.executor.extraClassPath=pathToSqliteJar
    --driver-class-path pathToSqliteJar
    compiledAppJar pathToSqliteDB outputDirWithTrailingSlash
```
The extra parameters `--conf spark.executor.extraClassPath` and `--driver-class-path` are to place the sqlite jdbc driver on the classpath of both the driver and the executors. For the time being, this is fine. We run the ETL job locally, not on EMR.

This creates two `parquet` files on the output directory, partitioned by `tag`.

### Run Classifier Pipeline
- For quick end-to-end testing, set `threshold` to 30. This reduces the size of training dataset. For a proper run, set it to 12 or 13. This takes a lot of time.
- Upload the parquet files to S3.
- Run a Spark job (on EMR) as follows:
```
./bin/spark-submit
    --class TextClassifier
    compiledAppJar-s3Path orig-q-s3Path dup-q-s3Path output-s3Path
```