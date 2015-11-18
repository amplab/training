---
layout: global
title: Data Exploration Using Spark SQL
categories: [module]
navigation:
  weight: 60
  show: true
skip-chapter-toc: true
---

Spark SQL is the newest component of Spark and provides a SQL like interface.
Spark SQL is tightly integrated with the the various spark programming languages 
so we will start by launching the Spark shell from the root directory of the provided USB drive:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ spark/bin/spark-shell</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ spark/bin/pyspark</pre>
</div>
</div>


Now we can load a data frame in that is stored in the Parquet format.  Parquet is a self-describing columnar file format.  Since it is self-describing, Spark SQL will automatically be able to infer all of the column names and their datatypes. For this exercise we have provided a set of data that contains all of the pages on wikipedia that contain the word "berkeley".  You can load this data using the input methods provided by `SQLContext`.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala> val wikiData = sqlContext.read.parquet("data/wiki.parquet")
...
wikiData: org.apache.spark.sql.DataFrame = [id: int, title: string, modified: bigint, text: string, username: string]
</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
>>> wikiData = sqlContext.read.parquet("data/wiki.parquet")
</pre>
</div>
</div>

The result of loading in a parquet file is a SchemaRDD.  A SchemaRDD has all of the functions of a normal RDD.  For example, lets figure out how many records are in the data set.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala> wikiData.count()
res: Long = 39365
</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
>>> wikiData.count()
39365
</pre>
</div>
</div>

In addition to standard RDD operatrions, SchemaRDDs also have extra information about the names and types of the columns in the dataset.  This extra schema information makes it possible to run SQL queries against the data after you have registered it as a table.  Below is an example of counting the number of records using a SQL query.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala> wikiData.registerTempTable("wikiData")
scala> val countResult = sqlContext.sql("SELECT COUNT(*) FROM wikiData").collect()
countResult: Array[org.apache.spark.sql.Row] = Array([39365])
</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
>>> wikiData.registerTempTable("wikiData")
>>> result = sqlContext.sql("SELECT COUNT(*) AS pageCount FROM wikiData").collect()
</pre>
</div>
</div>

The result of SQL queries is always a collection of Row objects.  From a row object you can access the individual columns of the result.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala> val sqlCount = countResult.head.getLong(0)
sqlCount: Long = 39365
</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
>>> result[0].pageCount
39365</pre>
</div>
</div>

SQL can be a powerful tool from performing complex aggregations.  For example, the following query returns the top 10 usersnames by the number of pages they created.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala> sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect().foreach(println)
[Waacstats,2003]
[Cydebot,949]
[BattyBot,939]
[Yobot,890]
[Addbot,853]
[Monkbot,668]
[ChrisGualtieri,438]
[RjwilmsiBot,387]
[OccultZone,377]
[ClueBot NG,353]
</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
>>> sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect()
[Row(username=u'Waacstats', cnt=2003), Row(username=u'Cydebot', cnt=949), Row(username=u'BattyBot', cnt=939), Row(username=u'Yobot', cnt=890), Row(username=u'Addbot', cnt=853), Row(username=u'Monkbot', cnt=668), Row(username=u'ChrisGualtieri', cnt=438), Row(username=u'RjwilmsiBot', cnt=387), Row(username=u'OccultZone', cnt=377), Row(username=u'ClueBot NG', cnt=353)]
</div>
</div>

__NOTE: java.lang.OutOfMemoryError__ : If you see a `java.lang.OutOfMemoryError`, you will need to restart the Spark shell with the following command line option:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ spark/bin/spark-shell --driver-memory 1G</pre>
</div>
<div data-lang="python" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ spark/bin/pyspark --driver-memory 1G</pre>
</div>
</div>

This increases the amount of memory allocated for the Spark driver. Since we are running Spark in local mode, all operations are performed by the driver, so the driver memory is all the memory Spark has to work with.

- How many articles contain the word "california"?

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-sql">SELECT COUNT(*) FROM wikiData WHERE text LIKE '%california%'</pre>
   </div>


