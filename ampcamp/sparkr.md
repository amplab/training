---
layout: global
title: Interactive Data Analytics in SparkR
categories: [module]
navigation:
  weight: 60
  show: true
skip-chapter-toc: true
---

#### TODOs & FIXMEs
- Adapt/change all the text descriptions.
- Dry runs.
- Installation process.
- Add a table of contents like the [GraphX exercise](graph-analytics-with-graphx.html).
- Add instruction for RStudio?
- `localhost:8080` doesn't show the UI?

## Prerequisite: getting the dataset
<pre class="prettyprint lang-bsh">
# first, cd into the root directory of the USB drive
$ mkdir data/pagecounts && cd data/pagecounts
# download two text files from S3
$ wget -c http://s3.amazonaws.com/ampcamp-data/wikistats_20090505_restricted-01/part-00001
$ wget -c http://s3.amazonaws.com/ampcamp-data/wikistats_20090505_restricted-07/part-00053
# you're good to go!
</pre>

In this chapter, we will first use the Spark shell to interactively explore the Wikipedia data.
Then, we will give a brief introduction to writing standalone Spark programs. Remember, Spark is an open source computation engine built on top of the popular Hadoop Distributed File System (HDFS).

## Interactive Analysis

Let's now use Spark to do some order statistics on the data set.
First, launch the Spark shell:

<pre class="lang-bash">
/root/SparkR/sparkR # FIXME: add installation instructions
</pre>

The prompt should appear within a few seconds. __Note:__ You may need to hit `[Enter]` once to clear the log output.

1. Warm up by creating an RDD (Resilient Distributed Dataset) named `pagecounts` from the input files.
   In the Spark shell, the SparkContext is already created for you as variable `sc`.

     <pre class="prettyprint lang-r">
> sc
[1] "Java-Object{org.apache.spark.api.java.JavaSparkContext@7a856d3b}" 
# FIXME: fix below by running on actual file
> pagecounts <- textFile(sc, "data/pagecounts")
> pagecounts
An object of class "RDD"
Slot "env":
<environment: 0x7fae86040838>
Slot "jrdd":
[1] "Java-Object{data/pagecounts MappedRDD[1] at textFile at <unknown>:0}"</pre>

2. Let's take a peek at the data. You can use the take operation of an RDD to get the first K records. Here, K = 10.

     <pre class="prettyprint lang-r">> take(pagecounts, 10)
...
[[1]]
[1] "20090505-000000 aa Main_Page 2 9980"
[[2]]
[1] "20090505-000000 ab %D0%90%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82 1 465"
[[3]]
[1] "20090505-000000 ab %D0%98%D1%85%D0%B0%D0%B4%D0%BE%D1%83_%D0%B0%D0%B4%D0%B0%D2%9F%D1%8C%D0%B0 1 16086"
[[4]]
[1] "20090505-000000 af.b Tuisblad 1 36236"
[[5]]
[1] "20090505-000000 af.d Tuisblad 4 189738"
[[6]]
[1] "20090505-000000 af.q Tuisblad 2 56143"
[[7]]
[1] "20090505-000000 af Afrika 1 46833"
[[8]]
[1] "20090505-000000 af Afrikaans 2 53577"
[[9]]
[1] "20090505-000000 af Australi%C3%AB 1 132432"
[[10]]
[1] "20090505-000000 af Barack_Obama 1 23368"</pre>

2. Let's see how many records in total are in this data set (this command will take a while, so read ahead while it is running).

     <pre class="prettyprint lang-r">
> count(pagecounts)</pre>

   This should take about 10 seconds.

   FIXME: This should launch 177 (FIXME) Spark tasks on the Spark cluster.
   If you look closely at the terminal, the console log is pretty chatty and tells you the progress of the tasks.
   Because we are reading 20G of data from HDFS, this task is I/O bound and can take a while to scan through all the data (2 - 3 mins).

   While it's running, you can open the Spark web console to see the progress.
   To do this, open your favorite browser, and type in the following URL.

   `http://<master_node_hostname>:4040`

   Note that this page is only available if you have an active job or Spark shell.  
   You should have been given `master_node_hostname` at the beginning of the
   tutorial, or you might have [launched your own
   cluster](launching-a-bdas-cluster-on-ec2.html) and made a note of it then. You should
   see the Spark application status web interface, similar to the following:

   ![Spark Application Status Web UI](img/application-webui640.png)

   The links in this interface allow you to track the job's progress and
   various metrics about its execution, including task durations and cache
   statistics.

   In addition, the Spark Standalone cluster status web interface displays
   information that pertains to the entire Spark cluster.  To view this UI,
   browse to

   `http://<master_node_hostname>:8080`

   You should see a page similar to the following (yours will probably show five slaves):

   ![Spark Cluster Status Web UI](img/standalone-webui640.png)

   When your query finishes running, it should return the following count:

       [1] 1398882

4. Recall from above when we described the format of the data set, that the second field is the "project code" and contains information about the language of the pages.
   For example, the project code "en" indicates an English page.
   Let's derive an RDD containing only English pages from `pagecounts`.
   This can be done by applying a filter function to `pagecounts`.
   For each record, we can split it by the field delimiter (i.e. a space) and get the second field-– and then compare it with the string "en".

   To avoid reading from disks each time we perform any operations on the RDD, we also __cache the RDD into memory__.
    This is where Spark really starts to to shine.

   <pre class="prettyprint lang-r">> enPages <- Filter(function(x) { unlist(strsplit(x, " "))[[2]] == "en" }, pagecounts)
> cache(enPages)
...(some metadata output that can be ignored)...</pre>

   When you type this command into the Spark shell, Spark defines the RDD, but because of lazy evaluation, no computation is done yet.
   Next time any action is invoked on `enPages`, Spark will cache the data set in memory across the 5 slaves in your cluster.

5. How many records are there for English pages?

   <pre class="prettyprint lang-r">> count(enPages)
[1] 970545</pre>

   The first time this command is run, it will take about 30 seconds to 1 minute while Spark scans through the entire data set on disk.
   __But since enPages was marked as "cached" in the previous step, if you run count on the same RDD again, it should return an order of magnitude faster__.

6. Let's try something fancier.
   Generate a histogram of total page views on Wikipedia English pages for the date range represented in our dataset (May 5 to May 7, 2009).
   The high level idea of what we'll be doing is as follows.
   First, we generate a key value pair for each line; the key is the date (the first eight characters of the first field), and the value is the number of pageviews for that date (the fourth field).

   <pre class="prettyprint lang-r"># lapply() and map() are aliases of each other; both should work!
> enTuples <- lapply(enPages, function(x) { unlist(strsplit(x, " ")) })
> enKeyValuePairs <- lapply(enTuples, 
                            function(x) { 
                              list(substr(x[[1]], 0, 8), as.integer(x[[4]])) 
                            })
   </pre>

   Next, we shuffle the data and group all values of the same key together.
   Finally we sum up the values for each key.
   There is a convenient method called `reduceByKey` in Spark for exactly this pattern.
   Note that the second argument to `reduceByKey` determines the number of reducers to use.
   By default, Spark assumes that the reduce function is commutative and associative and applies combiners on the mapper side.
   Since we know there is a very limited number of keys in this case (because there are only 2 unique dates in our data set), let's use only one reducer. 
   
   **Note.** This will take a couple of minutes (on our laptop it takes two and a half) depending on your machine setup. If it's too slow, try increasing the number of reducers to a larger value (say, `5L`).

   <pre class="prettyprint lang-r"># FIXME: 1 reducer is awefully slow; 5 is better but still slow
> collect(reduceByKey(enKeyValuePairs, "+", 1L))
[[1]]
[[1]][[1]]
[1] "20090507"
[[1]][[2]]
[1] 6175726
[[2]]
[[2]][[1]]
[1] "20090505"
[[2]][[2]]
[1] 7076855</pre>


   We can combine the previous three commands into one:

   <pre class="prettyprint lang-r"># TODO: this is cumbersome; consider using the magrittr package to simplify pipelining? 
> collect(
      reduceByKey(
        lapply(
          lapply(enPages, function(l) { unlist(strsplit(l, " ")) }),
          function(l) { list(substr(l[[1]], 0, 8), as.integer(l[[4]])) }),
        "+", 1L))</pre>

7. Suppose we want to find pages that were viewed more than 200,000 times during the three days covered by our dataset.
   Conceptually, this task is similar to the previous query.
   But, given the large number of pages (23 million distinct page names), the new task is very expensive.
   We are doing an expensive group-by with a lot of network shuffling of data.

   To recap, first we split each line of data into its respective fields.
   Next, we extract the fields for page name and number of page views.
   We reduce by key again, this time with 40 reducers.
   Then we filter out pages with less than 200,000 total views over our time window represented by our dataset.
   
   **FIXME**: this is really slow.

   <pre class="prettyprint lang-r">pageNameAndView <- 
        lapply(enPages, 
          function(l) { 
            splits <- unlist(strsplit(l, " "))
            list(splits[[3]], as.integer(splits[[4]]))
          })
reduced <- reduceByKey(pageNameAndView, "+", 4L)
filteredByView <- Filter( function(x) { x[[2]] > 6000 }, reduced)
collect(lapply(filteredByView, function(x) { list(x[[2]], x[[1]] )}))</pre>

   There is no hard and fast way to calculate the optimal number of reducers for a given problem; you will
   build up intuition over time by experimenting with different values.

   To leave the Spark shell, type `exit` at the prompt.

8. You can explore the full RDD API by browsing the [Java/Scala](http://www.cs.berkeley.edu/~pwendell/strataconf/api/core/index.html#spark.RDD) or [Python](http://www.cs.berkeley.edu/~pwendell/strataconf/api/pyspark/index.html) API docs.

## Running Standalone Spark Programs

Because of time constraints, in this tutorial we focus on ad-hoc style analytics using the Spark shell.
However, for many tasks, it makes more sense to write a standalone Spark program.
We will return to this in the section on Spark Streaming below, where you will actually write a standalone Spark Streaming job.
We aren't going to cover how to structure, build, and run standalone Spark jobs here, but before we move on, we list here a few resources about standalone Spark jobs for you to come back and explore later.

First, on the AMI for this tutorial we have included "template" projects for Scala and Java standalone programs for both Spark and Spark streaming.
The Spark ones can be found in the `/root/scala-app-template` and `/root/java-app-template` directories (we will discuss the Streaming ones later).
Feel free to browse through the contents of those directories. You can also find examples of building and running Spark standalone jobs <a href="http://www.spark-project.org/docs/latest/quick-start.html#a-standalone-job-in-java">in Java</a> and <a href="http://www.spark-project.org/docs/latest/quick-start.html#a-standalone-job-in-scala">in Scala</a> as part of the Spark Quick Start Guide. For even more details, see Matei Zaharia's <a href="http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-2-amp-camp-2012-standalone-programs.pdf" target="_blank">slides</a> and <a href="http://www.youtube.com/watch?v=7k4yDKBYOcw&t=59m37s" target="_blank">talk video</a> about Standalone Spark jobs at the <a href="http://ampcamp.berkeley.edu/agenda-2012" target="blank">first AMP Camp</a>.
