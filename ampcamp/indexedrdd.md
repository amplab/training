---
layout: global
title: Updating RDDs with IndexedRDD
categories: [module]
navigation:
  weight: 65
  show: true
---

{:toc}

IndexedRDD is an updatable key-value store for Spark. It enables efficient keyed
lookups, updates, deletions, and joins for key-value pair RDDs.

IndexedRDD works by hash-partitioning each entry by key and maintaining a
specialized radix tree index ([PART](https://github.com/ankurdave/part)) within
each partition. Like all other in-memory storage options for RDDs, the PART data
structure is immutable. However, it uses copy-on-write to enable efficient
updates without modifying the existing version in any way. Updates return a
lightweight copy of the partition that internally shares almost all of the
structure of the existing version.

## Tracking Wikipedia View Counts with IndexedRDD

IndexedRDD can be useful when you have a large RDD that needs to be updated in
smaller batches. Unlike regular RDDs, which make a full copy of the data even
for a small change, IndexedRDD applies small updates much more efficiently.

We'll use IndexedRDD to track and analyze views on Wikipedia articles.

### Getting Started

First, we need to launch the Spark Shell with the IndexedRDD package its
dependencies:

<pre class="prettyprint lang-bsh">
usb/$ bin/spark-shell --packages amplab:spark-indexedrdd:0.3 \
        --repositories https://raw.githubusercontent.com/ankurdave/maven-repo/master
</pre>

Next we'll import IndexedRDD. The second import statement brings in the built-in
*key serializers* so the PART data structure can store common key types like
`Long` and `String`.

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
~~~
</div>
</div>

### Load the Wikipedia Articles

Wikipedia provides [XML dumps](http://en.wikipedia.org/wiki/Wikipedia:Database_download#English-language_Wikipedia) of all articles in the encyclopedia. The latest dump is 44 GB, so we have preprocessed and filtered it (using Spark and [GraphX](graph-analytics-with-graphx.html)) to fit on your USB drive. We extracted all articles with "Berkeley" in the title, as well as all articles linked from and linking to those articles.

We'll load the Wikipedia articles as a key-value pair RDD where the key is the article ID and the value is the article title, then convert it into an IndexedRDD.

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val articlesRDD = sc.textFile("data/indexedrdd/wiki-article-titles.txt").map {
  line =>
    val fields = line.split('\t')
    (fields(0).toLong, fields(1))
}

// Construct an IndexedRDD from the articles, hash-partitioning and indexing
// the entries by article ID.
val articles = IndexedRDD(articlesRDD).cache()
~~~
</div>
</div>

### Look at the Article Titles

Now we can perform key-based operations efficiently, such as looking up, updating, and deleting articles by their ID:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
// Update a single article.
val articles2 = articles.put(0L, "Test article").cache()

// Look up the article. Note that the original IndexedRDD remains unmodified.
articles2.get(0L) // => Some("Test article")
articles.get(0L) // => None

// Delete some articles.
val articles3 = articles2.delete(
  Array(1642702735464155009L, 4109462296415417088L))
articles2.get(1642702735464155009L) // => Some(Stanford University)
articles3.get(1642702735464155009L) // => None
~~~
</div>
</div>

### Track Views by Article

Let's create another IndexedRDD to track how many times someone views each article. We'll create a view count for each article and initialize it to zero. Notice that we store this IndexedRDD in a `var` so we can keep it up to date with the latest view counts.

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
var views = articles.mapValues(title => 0)
~~~
</div>
</div>

In a real application, the view data might come from web server logs, but here we have it in a series of files. Let's load each file, then update the view counts using a join:
<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
def updateViews(updatesPath: String): Unit = {
  val viewUpdates = sc.textFile(updatesPath).map(id => (id.toLong, 1))
  views = views.fullOuterJoin(viewUpdates) {
    (id, aOpt, bOpt) => aOpt.getOrElse(0) + bOpt.getOrElse(0)
  }
}

updateViews("data/indexedrdd/wiki-views-1.txt")
updateViews("data/indexedrdd/wiki-views-2.txt")
updateViews("data/indexedrdd/wiki-views-3.txt")
~~~
</div>
</div>

### Analyze the Results

Now we can look at the resulting view counts. We'll also want to know the title
for a given article, and we'll use the fact that we can efficiently join
multiple IndexedRDDs without needing to shuffle any data:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val titlesAndViews = articles.innerJoin(views) {
  (id, title, views) => (title, views)
}
titlesAndViews.get(8395060451189818649L) // => Some((Saul Perlmutter,1))
~~~
</div>
</div>

Finally, we can use standard RDD operations on IndexedRDDs as well. Let's aggregate the view counts by keyword to find what keywords got the most views:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val viewsByKeyword = titlesAndViews.flatMap {
  case (id, (title, views)) => title.split(' ').map(word => (word, views))
}.reduceByKey(_ + _)
viewsByKeyword.top(10)(Ordering.by(_._2))
// => Array((of,34), (List,16), (in,11), (and,10), (the,8), (Berkeley,8), (California,6), (University,6), (The,5), (United,5))
~~~
</div>
</div>

And that's it! This brings us to the end of this IndexedRDD chapter of the
tutorial. To use IndexedRDD in your application, check out its
[Spark Package listing](http://spark-packages.org/package/amplab/spark-indexedrdd).
