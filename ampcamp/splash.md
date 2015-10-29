---
layout: global
title: Use Splash to run stochastic learning algorithms
categories: [module]
navigation:
  weight: 90
  show: true
skip-chapter-toc: true
---


<h1 id="what-is-stochastic-algorithm">What is a stochastic learning algorithm?</h1>

<p>Stochastic learning algorithms are a broad family of algorithms that process a large dataset by sequential processing of random samples of the dataset. Since their per-iteration computation cost is independent of the overall size of the dataset, stochastic algorithms can be very efficient in the analysis of large-scale data. Examples of stochastic algorithms include:</p>

<ul>
  <li><a href="http://en.wikipedia.org/wiki/Stochastic_gradient_descent">Stochastic Gradient Descent (SGD)</a></li>
  <li><a href="http://www.jmlr.org/papers/volume14/shalev-shwartz13a/shalev-shwartz13a.pdf">Stochastic Dual Coordinate Ascent (SDCA)</a></li>
  <li><a href="http://en.wikipedia.org/wiki/Markov_chain_Monte_Carlo">Markov Chain Monte Carlo (MCMC)</a></li>
  <li><a href="http://en.wikipedia.org/wiki/Gibbs_sampling">Gibbs Sampling</a></li>
  <li><a href="http://www.columbia.edu/~jwp2128/Papers/HoffmanBleiWangPaisley2013.pdf">Stochastic Variational Inference</a></li>
  <li><a href="http://research.microsoft.com/en-us/um/people/minka/papers/ep/minka-ep-uai.pdf">Expectation Propagation</a></li>
</ul>

<br>

<h1 id="what-is-splash">What is Splash?</h1>

<p>Stochastic learning algorithms are generally defined as sequential procedures and as such they can be difficult to parallelize. <strong>Splash</strong> is a general framework for parallelizing stochastic learning algorithms on multi-node clusters. You can develop a stochastic algorithm using the Splash programming interface without worrying about issues of distributed computing. The parallelization is automatic and it is communication efficient. Splash is built on <a href="http://www.scala-lang.org/">Scala</a> and <a href="https://spark.apache.org/">Apache Spark</a>, so that you can employ it to process <a href="https://spark.apache.org/docs/latest/quick-start.html">Resilient Distributed Datasets (RDD)</a>.</p>

<p>On large-scale datasets, Splash can be substantially faster than existing data analytics packages built on Apache Spark. For example, to fit a 10-class logistic regression model on the <a href="http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#mnist8m">mnist8m dataset</a>, stochastic gradient descent (SGD) implemented with Splash is 25x faster than <a href="https://spark.apache.org/docs/latest/mllib-optimization.html#l-bfgs">MLlib’s L-BFGS</a> and 75x faster than <a href="https://spark.apache.org/docs/latest/mllib-optimization.html#gradient-descent-and-stochastic-gradient-descent">MLlib’s mini-batch SGD</a> for achieving the same value of the loss function. All algorithms run on a 64-core cluster.</p>

<p align="center">
<img src="https://raw.githubusercontent.com/zhangyuc/splash/master/images/compare-with-lbfgs.png" width="400" />
</p>

<br>

# Download the example package

First, download the [Splash Example package](https://github.com/zhangyuc/splash/blob/master/examples/SplashExample.tar.gz?raw=true) and extract it at any directory. The source code locates at `/src/main/scala/`. The Splash library file is at `/lib/`, which puts Splash in your project classpath. To compile the code, `cd` into the directory where you extract the package and type:

{% highlight bash %}
sbt package
{% endhighlight %}

This generates a jar file at `./target/scala-2.10/splashexample.jar`. To run the code, submit this jar file as a Spark job:

{% highlight bash %}
YOUR_SPARK_HOME/bin/spark-submit --class ExampleName \
  --driver-memory 4G \
  --jars lib/splash-0.1.0.jar target/scala-2.10/splashexample.jar \
  [data files] > output.txt
{% endhighlight %}

Here, **YOUR_SPARK_HOME** should be replaced by the directory that Spark is installed; **ExampleName** should be replaced by the name of the example (see the following sections). The file `splash-0.1.0.jar` is the Splash library and `splashexample.jar` is the compiled code to be executed. The arguments `[data files]` should be replaced by the path of data files (see the following sections). The result is written to `output.txt`.

<br>

# Example 1: document statistics

The Document Statistics example computes the number of lines, words and characters of a text file. It illustrates how to write an application using Splash's programming interface. Before running this example, let's take a look at the source code:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import org.apache.spark.{SparkConf,SparkContext}
import splash.core.ParametrizedRDD

object DocumentStatistics {
  def main(args: Array[String]) {
    val path = args(0)
    val sc = new SparkContext(new SparkConf())
    
    val paramRdd = new ParametrizedRDD(sc.textFile(path))
    val sharedVar = paramRdd.setProcessFunction((line, weight, sharedVar, localVar) => {
      sharedVar.add("lines", weight)
      sharedVar.add("words", weight * line.split(" ").length)
      sharedVar.add("characters", weight * line.length)
    }).run().getSharedVariable()
    
    println("Lines: " + sharedVar.get("lines").toLong)
    println("words: " + sharedVar.get("words").toLong)
    println("Characters: " + sharedVar.get("characters").toLong)
  }
}
~~~
</div>
</div>

Using this piece of code, we summarize the procedure to write a Splash program:

1. Declear a **ParametrizedRDD** object called `paramRdd`. A **ParametrizedRDD** is a wrapper of the standard RDD. It maintains both the RDD elements and the variables to be updated by the stochastic algorithm.
2. Implement the stochastic algorithm by providing a data processing function to the **setProcessFunction** method. In this example, the algorithm is implemented in line 11-13.
3. Start running the algorithm by calling the **run** method. It makes the data processing function taking a full pass over the dataset. You can call the **run** method mutliple times to take many passes over the dataset. If there are mutliple cores available, the data processing will be automatically parallelized.
4. After the algorithm terminates, collect the variable values using the **getSharedVariable** method.

Now let's take a closer look at line 11-13:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
(line, weight, sharedVar, localVar) => {
  sharedVar.add("lines", weight)
  sharedVar.add("words", weight * line.split(" ").length)
  sharedVar.add("characters", weight * line.length)
}
~~~
</div>
</div>

The data processing function should take four arguments: (1) an element of the RDD; (2) the weight of this element; (3) the set of shared variables and (4) the set of local variables. These arguments are named as `(line, weight, sharedVar, localVar)`. The goal of the data processing function is to read these input to perform particular updates on the variable sets. This is precisely the goal of a stochastic algorithm. The weight of the element is automatically generated by the system. It tells the algorithm how important the element is. A weight of **x** indicates that the element has been consecutively observed for **x** times. 

The **shared variable** are global variables that are shared across the entire dataset. The **local variable** is only associated with this particular element. In this example we only use the shared variables. To read or write the variable set, the algorithm should use [operators](http://zhangyuc.github.io/splash/api/). This example uses the **add** operator. The two argument provides the key of the variable to be modified and the quantity to add. There are other types of operators: **get**, **multiply** and **delayedAdd**. The **get** operator returns the value of the variable. The **multiply** operator scales the variable by a constant factor. The **delayedAdd** operator declears an add operation but delays its execution to the future. See the [Splash API](http://zhangyuc.github.io/splash/api/) for more details. 

To output the results, the following code uses the **get** operator to get the final value of the shared variables, then print them to the console:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
println("Lines: " + sharedVar.get("lines").toLong)
println("words: " + sharedVar.get("words").toLong)
println("Characters: " + sharedVar.get("characters").toLong)
~~~
</div>
</div>

To run the example, please choose `ExampleName = DocumentStatistics` and `[data files] = data/covtype.txt`. The output should be like the following:

{% highlight scala %}
Lines: 581012
words: 7521450
Characters: 70024080
{% endhighlight %}

After you go through this example, you may write any application by yourself. Look at the [Logistic Regression example](http://zhangyuc.github.io/splash/example/#sgd-for-logistic-regression) for another concrete example of how to write the data processing function.

<br>

# Example 2: machine learning package

Splash contains a collection of pre-built packages for machine learning. One of them is the SGD algorithm for optimization. It provides an API similar to that of the [MLlib's optimization package](https://spark.apache.org/docs/latest/mllib-optimization.html). Although MLlib support mini-batch SGD, it doesn't support the native squential version of SGD which is usually much faster. Here is an example of using the SGD package of Splash to learn a logistic regression model:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionModel

object SGDExample {
  def main(args: Array[String]) {
    val path = args(0)
    
    val sc = new SparkContext(new SparkConf())
    val data = MLUtils.loadLibSVMFile(sc, path).repartition(sc.defaultParallelism)
    val numFeatures = data.take(1)(0).features.size
    
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    
    // Append 1 into the training data as intercept.
    val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()
    val test = splits(1).cache()
    
    println("Splash Optimization Example")
    println(training.count() + " samples for training and " + test.count() + " samples for testing.")
    println("Feature dimension = " + numFeatures)
    
    // Train a logistic regression model
    val NumIterations = 10
    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
    val weightsWithIntercept = (new splash.optimization.StochasticGradientDescent())
      .setGradient(new splash.optimization.LogisticGradient())
      .setNumIterations(NumIterations)
      .optimize(training, initialWeightsWithIntercept)
    
    val model = new LogisticRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1))
    
    // Clear the default threshold.
    model.clearThreshold()
    
    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    
    println("Area under ROC = " + auROC)
  }
}
~~~
</div>
</div>

If you compare this code with the [MLlib example](https://spark.apache.org/docs/latest/mllib-optimization.html#l-bfgs), you will see that the only difference is the following lines:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
// Train a logistic regression model
val NumIterations = 10
val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
val weightsWithIntercept = (new splash.optimization.StochasticGradientDescent())
  .setGradient(new splash.optimization.LogisticGradient())
  .setNumIterations(NumIterations)
  .optimize(training, initialWeightsWithIntercept)
~~~
</div>
</div>

While the MLlib examples uses the batch L-BFGS to optimize the loss function, this codes uses stochastic gradient descent. The above code first declears a **StochasticGradientDescent** object, then set the gradient function and the number of iterations. The **optimize** method returns the vector that minimizes the logsitic loss. In practice, the SGD implementation can be much faster than the batch L-BFGS if the dataset is large. Despite this difference, the input and output of the SGD package is the same as MLlib, so that it can be easily integrated as a part of the machine learning pipeline.

To run this example, please choose  `ExampleName = SGDExample` and `[data files] = data/covtype.txt`. The output should be like the following:

{% highlight scala %}
Splash Optimization Example
348579 samples for training and 232433 samples for testing.
Feature dimension = 54
Area under ROC = 0.8266852123167724
{% endhighlight %}

The machine learning pakcage provides efficient implementations for [Optimization](http://zhangyuc.github.io/splash/mlpackage/#stochastic-gradient-descent), [Collaborative Filtering](http://zhangyuc.github.io/splash/mlpackage/#collaborative-filtering-for-personalized-recommendation) and [Topic Modelling](http://zhangyuc.github.io/splash/mlpackage/#collapsed-gibbs-sampling-for-topic-modelling). Look at the [LDA example](http://zhangyuc.github.io/splash/example/#lda-via-ml-package) as another concrete example of how to use the machine learning package to learn a LDA model.
