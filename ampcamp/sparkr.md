---
layout: global
title: Interactive Data Analytics in SparkR
categories: [module]
navigation:
  weight: 75
  show: true
---

In this chapter, we will use the SparkR shell for interactive data exploration.
While the code snippets below assume Mac OS X, Linux and Windows should be supported as well.

## Preparing the Environment
Simply type in the following command from the root directory of your USB:

<pre class="prettyprint lang-bsh">
usb/$ spark/bin/sparkR
</pre>

This starts a `SparkR` shell, which by default comes with a `SparkContext` variable `sc` and a `SQLContext` variable `sqlContext`.

<pre class="prettyprint lang-r">
> sc
Java ref type org.apache.spark.api.java.JavaSparkContext id 0
> sqlContext
Java ref type org.apache.spark.sql.SQLContext id 1
</pre>

We are going to use the `iris` dataset that comes with `R` for the rest of this chapter. 
Load this into a `DataFrame` object as follows:

<pre class="prettyprint lang-r">
> irisDF <- createDataFrame(sqlContext, iris)
</pre>

Let's take a look at the schema of `irisDF` before we start exploring it.
<pre class="prettyprint lang-r">
> printSchema(irisDF)
root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- Species: string (nullable = true)
</pre>

At any point, you can look at the Web UI at [http://localhost:4040/](http://localhost:4040/) to see what's going on behind the scenes.

## Interactive Analysis

### Basic Operations

Let's take a peek at `irisDF`. 
You can use the `take` operation to get the first K records. 
Here, K = 2.

<pre class="prettyprint lang-r">
> take(irisDF, 2)
</pre>

You should see that a table of two rows is printed out, where the columns are from the schema we've seen earlier.
Each column has different datatypes. 
For example, the `Species` column (the 5th one) is of type `character`.

<pre class="prettyprint lang-r">
> typeof(take(irisDF, 2)[[5]])
[1] "character"
</pre>

We can refer to specific column using `$`:

<pre class="prettyprint lang-r">
> take(select(irisDF,irisDF$Species), 2)
</pre>

We can also refer to multiple columns:

<pre class="prettyprint lang-r">
> take(irisDF[, c("Petal_Length", "Sepal_Length")], 2)
</pre>

Now, let's see how many records in total are in this dataset.

<pre class="prettyprint lang-r">
> count(irisDF)
</pre>

You can visit the [Web UI](http://localhost:4040/) for further exploration.

Find the `count` job that just ran, and click on it under the description column. 
Feel free to look at the `Event Timeline` and `DAG Visualization` tabs in this job's details page. 

### Advanced Operations

We can filter on patterns using `like`.

For example, let's count the number of rows for the `Species` ''setosa''.

<pre class="prettyprint lang-r">
> count(filter(irisDF, like(irisDF$Species, "setosa")))
</pre>

We can also use the `where`.

For example, the number of rows with `Petal_Length` less than 1.2 are:

<pre class="prettyprint lang-r">
> count(where(irisDF, irisDF$Petal_Length < 1.2))
</pre>

Let's try to find out how many unique `Species` are there in this dataset along with their individual counts. 

We can do this by using a `groupby` and followed by aggregating them using `agg`:

<pre class="prettyprint lang-r">
> species <- agg(group_by(irisDF, irisDF$Species), count = n(irisDF$Species))
> head(species)
</pre>

We can sort the results by `Species` name:

<pre class="prettyprint lang-r">
> top_species <- head(arrange(species, asc(species$Species)))
</pre>

We can use `R`'s plotting capabilities from SparkR too:

<pre class="prettyprint lang-r">
> barplot(top_species$count, names.arg=top_species$Species)
</pre>


### Mutate DataFrame

You can create new columns on the dataframe. 
For example, create a `Petal_Area` column as follows:

<pre class="prettyprint lang-r">
> irisDF$Petal_Area <- irisDF$Petal_Length * irisDF$Petal_Width
</pre>

See the change in action:

<pre class="prettyprint lang-r">
> printSchema(irisDF)
root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- Species: string (nullable = true)
 |-- Petal_Area: double (nullable = true)
</pre>

Dropping a columns is simple as well!

<pre class="prettyprint lang-r">
> irisDF$Petal_Area <- NULL
> printSchema(irisDF)
root
 |-- Sepal_Length: double (nullable = true)
 |-- Sepal_Width: double (nullable = true)
 |-- Petal_Length: double (nullable = true)
 |-- Petal_Width: double (nullable = true)
 |-- Species: string (nullable = true)
</pre>

### Running SQL Queries

First, we need to register a table corresponding to `irisDF` DataFrame.

<pre class="prettyprint lang-r">
> registerTempTable(irisDF, "irisTable")
</pre>

Now we can perform any SparkSQL operations on the `irisTable` table.

For example, let's count the number of rows for the `Species` ''setosa'' one more time using SQL through SparkR.

<pre class="prettyprint lang-r">
> count(sql(sqlContext, "SELECT Species FROM irisTable WHERE Species LIKE 'setosa'"))
</pre>

This brings us to the end of the SparkR chapter of the tutorial.
You can explore the full API by using the command `help(package=SparkR)`.
