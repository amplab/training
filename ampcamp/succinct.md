---
layout: global
title: Querying compressed RDDs with Succinct Spark 
categories: [module]
navigation:
  weight: 60
  show: true
---

{:toc}

Succinct is a distributed data store that supports a wide range of point 
queries directly on a compressed representation of the input data. We are very
excited to release Succinct Spark, as a Spark package, that enables search, 
range and random access queries on compressed RDDs. This release allows users 
to use Apache Spark as a document store (with search on documents) similar to 
ElasticSearch, a key-value interface (with search on values) similar to 
HyperDex, and an experimental DataFrame interface (with search along columns in
a table).

## Creating a Succinct RDD

To start using Succinct's API, we need to start up the Spark Shell with the 
Succinct Spark package available to it. The following command directs the Spark
Shell to pull the package from the Spark Packages repository:

<pre class="prettyprint lang-bsh">
usb/$ bin/spark-shell --packages amplab:succinct:0.1.3
</pre>

Now that we have the Spark shell loaded with the Succinct Spark package, we'll
take a look at how we can query _compressed wikipedia articles_ using Succinct's
key-value API.  Let's start with importing the required classes:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import edu.berkeley.cs.succinct.kv._
~~~
</div>
</div>

The Succinct Spark package compresses certain regular RDDs into Succinct RDDs
creates data structures to enable queries on them. In this exercise we will
work with an RDD of `(articleID, article)` pairs, where each entry corresponds
to a single Wikipedia article.

Let's start with loading the Wikipedia articles into a regular RDD. The dataset
provided is stored as a CSV containing `(articleID,article)` pairs. The following
snipped loads the dataset into an RDD:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val wikiData = sc.textFile("data/succinct/wiki-articles.txt").map(_.split("|"))
~~~
</div>
</div>

While `SuccinctKVRDD`'s key class can be any _ordered_ type, the value is an
array of bytes. We will use `Long` keys:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val wikiKV = wikiData.map(entry => (entry(0).toLong, entry(1).getBytes)
~~~
</div>
</div>

Finally, we can now convert this RDD into a `SuccinctKVRDD`:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val succinctWikiKV = wikiKV.succinctKV
~~~
</div>
</div>

That's it! We now have a _compressed version of the RDD_ that supports
several interesting queries directly on the compressed RDD.

## Querying Succinct RDDs

Lets jump into querying our new compressed RDD. One of the key operations
on compressed RDDs in Succinct is `search(query)`. For instance, the following 
query obtains `articleID`s corresponding to all articles containing "Berkeley":

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD = succinctWikiKV.search("Berkeley")
~~~
</div>
</div>

Lets fetch the first 10 articleIDs:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIds = articleIdsRDD.take(10)
~~~
</div>
</div>

The `articleIds` themselves don't reveal much; we need to look at the
article text for them...

SuccinctKVRDD allows you to fetch the value corresponding to any key
through the familiar `get(key)` API. We'll use this API to fetch the
article contents corresponding to each of these `articleIds`:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
articleIds.foreach(key => {
	val valueBuf = succinctWikiKV.get(key)
	println("articleID = " + key + " article = " + new String(valueBuf))
}
~~~
</div>
</div>

This was a short overview of `SuccinctKVRDD`'s functionality using a small
Wikipedia dataset.

## Working with Larger Succinct RDDs 

In order to demonstrate the power of Succinct, we've
preprocessed a larger Wikipedia dataset into Succinct data structures 
and stored it on your USB drive. Lets try loading the preprocessed data
and running queries on it:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import edu.berkeley.cs.succinct.kv._
val wikiSuccinctKV2 = sc.succinctKV[Long]("data/succinct/succinct-wiki-artices") 
~~~
</div>
</div>

The command above directs Spark to load the `SuccinctKVRDD` with `Long` keys from the 
specified location on disk.

Our goal with the Succinct project is to push the boundaries of queries that can be executed
directly on compressed data. To this end, we've added support for _regular expression queries_ 
directly on compressed RDDs! The API is quite similar to the `search(query)` before:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articleIdsRDD2 = succinctWikiKV2.regexSearch("(stanford|berkeley)\\.edu")
val articleIds2 = articleIdsRDD2.take(10)
articleIds2.foreach(key => {
	val valueBuf = succinctWikiKV2.get(key)
	println("articleID = " + key + " article = " + new String(valueBuf))
}
~~~
</div>
</div>

And that's it! This brings us to the end of this Succinct chapter of the tutorial. To find out
more about Succinct, we encourage you to visit our [website](http://succinct.cs.berkeley.edu).