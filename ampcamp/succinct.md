---
layout: global
title: Querying compressed RDDs with Succinct Spark 
categories: [module]
navigation:
  weight: 60
  show: true
---

{:toc}

[Succinct](http://succinct.cs.berkeley.edu) is a distributed data store that supports a wide range of point 
queries directly on a compressed representation of the input data. In this exercise, we will work with Succinct Spark, a Spark package that enables search, range and random access queries directly on compressed RDDs. The package allows users to use Spark as a document store (with search on documents) similar to ElasticSearch, a key-value interface (with search on values) similar to HyperDex, and an experimental DataFrame interface (with search along columns in a table).

In this exercise, we will work with the key-value interface for a collection of Wikipedia articles, stored as an RDD of `(articleID, article)` pairs. The exercise is in three steps. First, we will practice constructing a Succinct RDD using a smaller Wikipedia dataset. Next, we will practice executing a set of queries directly on this compressed RDD will a focus on understanding the API exposed by Succinct Spark. Finally, we will work with a much larger Wikipedia dataset and observe some of the (memory and latency) benefits of Succinct Spark compared to native Spark.

## Creating a Succinct RDD

Let us begin by starting up the Spark Shell, with the 
Succinct Spark package jars available to it. The following command directs the Spark
Shell to load the jar for the Succinct Spark package, and increases the executor
memory to 2GB (since we will be working with large datasets later in the exercise):

<pre class="prettyprint lang-bsh">
usb/$ bin/spark-shell --jars jars/succinct/succinct-0.1.5.jar --executor-memory 2G --conf "spark.driver.extraJavaOptions=-XX:MaxPermSize=256m"
</pre>

To work with Succinct Spark later, we will need to import the required classes using the following:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import edu.berkeley.cs.succinct.kv._
~~~
</div>
</div>

As mentioned earlier, we will
work with an RDD of `(articleID, article)` pairs, where each entry corresponds
to a single Wikipedia article. The first step to using Succinct Spark is to create a regular RDD comprising of the Wikipedia articles.

The dataset
provided is stored as a CSV file of `(articleID, article)` pairs. The following
snippet loads the dataset and creates an RDD of key-value pairs:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val wikiData = sc.textFile("data/succinct/wiki-small.txt").map(_.split('|'))
val wikiKV = wikiData.map(entry => (entry(0).toLong, entry(1)))
~~~
</div>
</div>

Let us take a look at the number of documents we have in the RDD:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
wikiKV.count
~~~
</div>
</div>

Note that the number of articles in the smaller dataset is just 250. 

Now, let us do something more interesting. Suppose we want to find all `articleId`s whose corresponding `article`s contain
"Berkeley". One way to do this using a regular Spark RDD is to use the filter operation. For example, as follows:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD = wikiKV.filter(kvPair => kvPair._2.contains("Berkeley")).map(_._1)
articleIdsRDD.count
~~~
</div>
</div>

Note that there are only three artciles that contain "Berkeley". However, to find these articles, Spark has to scan through the entire RDD. 

Succinct Spark exposes a SuccinctKVRDD interface that enables the same functionality as above, but on a compressed representation of the RDD. Moreover, SuccinctKVRDD embeds an "index" within the compressed representation of the RDD that avoids scanning the entire RDD. 

Let us start by converting the Spark RDD into a Succinct Spark RDD `SuccinctKVRDD`. Note that the keys in original RDD can be of arbitrary type (`Long` in this example); however, we require each value to be
an array of bytes. We can transform such an RDD into a compressed representation `SuccinctKVRDD` as follows:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val succinctWikiKV = wikiKV.map(t => (t._1, t._2.getBytes)).succinctKV
~~~
</div>
</div>

We now have a _compressed version of the RDD_ that supports a number
of interactive point queries directly on a compressed representation of the original RDD.

## Querying Succinct RDDs

Given the compressed SuccinctKVRDD from above, we can now execute the same queries that we executed on the original RDD above. Let us start by ensuring that SuccinctKVRDD contains all the documents in the original uncompressed RDD:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
succinctWikiKV.count
~~~
</div>
</div>

The output should be 250, same as that is the original RDD. 

Let us now find all `articleId`s whose corresponding `article`s contain
"Berkeley", as we did earlier for the original RDD. SuccinctKVRDD exposes a simple API to do so -- `search(query)`, which provides functionality similar to the filter operation on the original uncompressed RDD, but avoids data scans while executing directly on the compressed representation:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD2 = succinctWikiKV.search("Berkeley")
articleIdsRDD2.count
~~~
</div>
</div>

As before, the number of articles containing "Berkeley" is 3. Now suppose we want to look at the articles that contain "Berkeley". SuccinctKVRDD allows one to fetch the value corresponding to any key
through the usual `get(key)` API. We'll use this API to fetch the
text for the articles that contain "Berkeley" : 

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIds = articleIdsRDD2.take(3)
articleIds.foreach(key => {
	val valueBuf = succinctWikiKV.get(key)
	println("articleID = " + key + " article = " + new String(valueBuf))
})
~~~
</div>
</div>

Note that reading the original data requires data decompression, of course. This is in fact a slow operation in Succinct Spark. We, hence, do not recommend using Succinct Spark for cases where the application needs to perform large (in tens or hundreds of megabytes) sequential reads of the original data.

The exercise so far allowed us to take a collection of Wikipedia articles, construct a Spark RDD, and compress this RDD into a SuccinctKVRDD that allows executing search and random access directly on compressed representation of the RDD. Let us now work with larger dataset sizes.

## Working with Larger RDDs

One of the current limitations of Succinct Spark is the preprocessing speed (converting an original Spark RDD into a compressed Succinct RDD), and we are continually working to make preprocessing faster and more memory efficient. For the purpose of this exercise, we have pre-processed ~600MB of Wikipedia
articles (available on your USB drive).

In this part of the exercise, we will repeat the operations above but on a much larger dataset. Let us start
by loading the dataset into an RDD and caching it in memory:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val wikiKV2 = sc.textFile("data/succinct/wiki-large.txt").map(_.split('|')).map(t => (t(0).toLong, t(1)))
wikiKV2.count
~~~
</div>
</div>

Note that the dataset now contains many more articles that before (300,000). Let us start by taking a look at the
storage footprint of the RDD at http://localhost:4040/storage/. This is what it should look like:

<img src="img/spark-storage.png" 
title="Spark RDD Storage" 
alt="Spark RDD Storage"
id="spark-storage"
/>

Note that, depending on the configuration of your machine, the number of 
partitions and/or the amount of data cached may vary.

Let us try executing the same search query as before on this dataset, using the 
filter operation:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD3 = wikiKV2.filter(kvPair => kvPair._2.contains("Berkeley")).map(_._1)
articleIdsRDD3.count
~~~
</div>
</div>

Please take a note of the query execution time. This should not be surprising as Spark has to scan all the 300,000 Wikipedia articles to find the ones that actually contain "Berkeley".

We are still working with original uncompressed Spark RDD. Let us now extract the corresponding articles (the `get(key)` functionality) using using the following snippet:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val article = wikiKV2.filter(kvPair => kvPair._1 == 0).map(_._2).collect()(0)
~~~
</div>
</div>

Again, please note the query execution time.

As earlier, let us now perform these queries on SuccinctKVRDD. We will use the preprocessed dataset (again, available on the USB drive). Before we load the 
SuccinctKVRDD, lets uncache the previous RDD:
 
<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
wikiKV2.unpersist()
~~~
</div>
</div>

Now let us load SuccinctKVRDD and execute queries on it:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val succinctWikiKV2 = sc.succinctKV[Long]("data/succinct/succinct-wiki-large")
succinctWikiKV2.count 
~~~
</div>
</div>

The command above directs Spark to load the `SuccinctKVRDD` with `Long` keys 
from the specified location on disk. The first thing we would like to note is that we have the same number of articles as in 
the uncompressed RDD, and that SuccinctKVRDD requires much lower storage footprint compared to original Spark RDD. To see how much
smaller, go back to http://localhost:4040/storage/; it might look something like 
this:

<img src="img/succinct-storage.png" 
title="Spark RDD Storage" 
alt="Spark RDD Storage"
id="spark-storage"
/>

In terms of the query execution latency, let us start with search:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD4 = succinctWikiKV2.search("Berkeley")
articleIdsRDD4.count
~~~
</div>
</div>

Note the time taken to execute the query; as we described earlier, Succinct avoids _full data scans_ to obtain the articles that contain "Bekeley" -- it embeds all the necessary indexing information within the compressed representation. This leads to both _reduced storage overheads_
as well as _low latency_ for search queries.

Let us take a look at few of the matching articles using the `get(key)` operation:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIds4 = articleIdsRDD4.take(10)
articleIds4.foreach(key => {
	val valueBuf = succinctWikiKV2.get(key)
	println("articleID = " + key + " article = " + new String(valueBuf))
})
~~~
</div>
</div>

Again, contrast the performance of `get(key)` for SuccinctKVRDD and Spark's 
native RDD -- the benefits arise due to Succinct's native support for 
_random access_ as opposed to scan based data retrieval in regular RDDs.

In addition, SuccinctKVRDD also supports random access _within values_ (that is, extracting a subset of the Wikipedia article rather than the entire article). For instance,
we can extract the first 100 bytes for the `article` with `articleId` 42 as follows:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val extractedData = new String(succinctWikiKV2.extract(42, 0, 100))
~~~
</div>
</div>

So far we have focused on exact matches and random access on compressed RDDs. The [Succinct Spark page] (http://succinct.cs.berkeley.edu/wp/wordpress/?page_id=8) lists a set of interfaces and corresponding APIs in Succinct Spark package. 

Our goal with the Succinct project is to push the boundaries of executing queries
directly on compressed data. To this end, we've added support for _regular expression queries_ 
directly on compressed RDDs! The supported operators include union (`R1|R2`), repeat (`R1+`, `R1*`), 
concat (`(R1)(R2)`) and wildcard (`R1.*R2`). The API is quite similar to the `search(query)` before:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD5 = succinctWikiKV2.regexSearch("(stanford|berkeley)\\.edu")
val articleIds5 = articleIdsRDD5.take(10)
articleIds5.foreach(key => {
	val valueBuf = succinctWikiKV2.get(key)
	println("articleID = " + key + " article = " + new String(valueBuf))
})
~~~
</div>
</div>

And that is it! This brings us to the end of the Succinct Spark chapter of the exercise. To find out
more about Succinct, we encourage you to visit our [website](http://succinct.cs.berkeley.edu). We would love to hear your feedback on this release of Succinct Spark, as well as, what new things you would like to see in future releases!
