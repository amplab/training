---
layout: global
title: Introduction to the Scala Shell
prev: overview-of-the-exercises.html 
next: data-exploration-using-spark.html
skip-chapter-toc: true
---

This chapter will teach you the basics of using the Scala shell and introduce you to functional programming with collections.

If you're already comfortable with Scala or plan on using the Python shell for the interactive Spark sections of this mini course, skip ahead to the next section.

This exercise is based on a great tutorial, <a href="http://www.artima.com/scalazine/articles/steps.html" target="_blank">First Steps to Scala</a>.
However, reading through that whole tutorial and trying the examples at the console may take considerable time, so we will provide a basic introduction to the Scala shell here. Do as much as you feel you need (in particular you might want to skip the final "bonus" question).

1. Launch the Scala console by typing:

   ~~~
   scala
   ~~~

1. Declare a list of integers as a variable called "myNumbers".

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> val myNumbers = List(1, 2, 5, 4, 7, 3)
   myNumbers: List[Int] = List(1, 2, 5, 4, 7, 3)
   </pre>
   </div>

1. Declare a function, `cube`, that computes the cube (third power) of an Int.
   See steps 2-4 of First Steps to Scala.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> def cube(a: Int): Int = a * a * a
   cube: (a: Int)Int
   </pre>
   </div>

1. Apply the function to `myNumbers` using the `map` function. Hint: read about the `map` function in <a href="http://www.scala-lang.org/api/current/index.html#scala.collection.immutable.List" target="_blank">the Scala List API</a> and also in Table 1 about halfway through the <a href="http://www.artima.com/scalazine/articles/steps.html" target="_blank">First Steps to Scala</a> tutorial.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> myNumbers.map(x => cube(x))
          res: List[Int] = List(1, 8, 125, 64, 343, 27)
          // Scala also provides some shorthand ways of writing this:
          // myNumbers.map(cube(_))
          // myNumbers.map(cube)
   </pre>
   </div>

1. Then also try writing the function inline in a `map` call, using closure notation.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> myNumbers.map{x => x * x * x}
   res: List[Int] = List(1, 8, 125, 64, 343, 27)
   </pre>
   </div>

1. Define a `factorial` function that computes n! = 1 * 2 * ... * n given input n.
   You can use either a loop or recursion, in our solution we use recursion (see steps 5-7 of <a href="http://www.artima.com/scalazine/articles/steps.html" target="_blank">First Steps to Scala</a>).
   Then compute the sum of factorials in `myNumbers`. Hint: check out the `sum` function in <a href="http://www.scala-lang.org/api/current/index.html#scala.collection.immutable.List" target="_blank">the Scala List API</a>.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> def factorial(n:Int):Int = if (n==0) 1 else n * factorial(n-1) // From http://bit.ly/b2sVKI
   factorial: (Int)Int
   scala&gt; myNumbers.map(factorial).sum
   res: Int = 5193
   </pre>
   </div>

1. <i>**BONUS QUESTION.** This is a more challenging task and may require 10 minutes or more to complete, so you should consider skipping it depending on your timing so far.</i> Do a wordcount of a textfile. More specifically, create and populate a Map with words as keys and counts of the number of occurrences of the word as values.

   You can load a text file as an array of lines as shown below:

   <pre class="prettyprint lang-scala linenums">
   import scala.io.Source
   val lines = Source.fromFile("/home/imdb_1/spark/spark-0.7.0/README.md").getLines.toArray
   </pre>

   Then, instantiate a `collection.mutable.HashMap[String,Int]` and use functional methods to populate it with wordcounts. Hint, in our solution, we use <a href="http://richard.dallaway.com/in-praise-of-flatmap" target="_blank">`flatMap`</a> and then `map`.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-scala">
   scala> import scala.io.Source
   import scala.io.Source

   scala> val  lines = Source.fromFile("/home/imdb_1/spark/spark-0.7.0/README.md").getLines.toArray
   lines: Array[String] = Array(# Spark, "", Lightning-Fast Cluster Computing - <http://www.spark-project.org/>, "", "", ## Online Documentation, "", You can find the latest Spark documentation, including a programming, guide, on the project webpage at <http://spark-project.org/documentation.html>., This README file only contains basic setup instructions., "", "", ## Building, "", Spark requires Scala 2.9.2. The project is built using Simple Build Tool (SBT),, which is packaged with it. To build Spark and its example programs, run:, "", "    sbt/sbt package", "", To run Spark, you will need to have Scala's bin directory in your `PATH`, or, you will need to set the `SCALA_HOME` environment variable to point to where, you've installed Scala. Scala must be accessible through one of these, met...

   scala> val counts = new collection.mutable.HashMap[String, Int].withDefaultValue(0)
   counts: scala.collection.mutable.Map[String,Int] = Map()

   scala> lines.flatMap(line => line.split(" ")).map(word => counts(word) += 1)
   res: Array[Unit] = Array((), (), (), (), (), (), (), (),...

   scala> counts
   res2: scala.collection.mutable.Map[String,Int] = Map(so. -> 1, request, -> 1, will -> 4, Documentation -> 1, might -> 1, webpage -> 1, accessible -> 1, Linux. -> 1, master. -> 1, `SCALA_HOME` -> 1, `project/SparkBuild.scala`, -> 1, `/usr/local/lib/libmesos.so` -> 1, To -> 3, is -> 5, Cluster -> 1, requires -> 1, (only -> 1, gladly -> 1, original -> 2, configure -> 1, example -> 3, a -> 5, for -> 1, programs -> 1, locally -> 3, two -> 1, contribution -> 1, with -> 4, Building -> 1, Please -> 1, one -> 3, guide -> 1, given. -> 1, legal -> 1, by -> 2, accepted -> 1, version -> 2, built -> 1, take -> 1, submitting -> 1, Each -> 1, your -> 4, have -> 2, # -> 1, For -> 2, - -> 3, Scala. -> 1, documentation, -> 1, against -> 1, rebuilding -> 1, <http://www.spark-project.org/> -> 1, including -...
   </pre>
   </div>
