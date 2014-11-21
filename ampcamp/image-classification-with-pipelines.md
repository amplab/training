---
layout: global
title: Image Classification with Pipelines
categories: [module]
navigation:
  weight: 80
  show: true
---

**IMPORTANT** Before you begin reading, you should begin downloading
the ML Pipelines exercise materials:

  * On-site participants: [ampcamp-pipelines.zip](http://10.225.217.159/ampcamp-pipelines.zip)
  * Remote participants: [ampcamp-pipelines.zip](http://d12yw77jruda6f.cloudfront.net/ampcamp-pipelines.zip)

In this chapter, we'll use a preview of the ML pipelines framework to build an
image classifier. The goal of the application we're building is to take an
input image and automatically determine what is in it - e.g. a picture of a
bird should return the class "bird." While image classification is one
application that can be supported with the pipelines framework, we hope you'll
leave this exercise convinced that it will be useful for other tasks.

Before getting to the details of the classification task, let's quickly review
the goals of the project and the underlying principles of the framework.

##The Pipelines Framework

The goal of the pipelines project is to provide a framework for feature
extraction and machine learning that encourages modular design and the
composition of complex learning systems based on simple, reusable, and
understandable components.

Once built - a pipeline should offer a way of taking input data through a
potentially complicated pre-processing and prediction process in a way that is
transparent to end-users. That is, a complete pipeline becomes a black box
that I can throw data in the form of images, speech, text, click logs, etc.
into and get out predictions.

While the inner workings of a pipeline should be transparent to end-users -
they should be highly understandable to the people responsible for producing
them. Today you'll play the role of a pipeline builder.

The pipelines framework you'll be using is based on a few pretty simple principles:

1. A *pipeline* is made of *nodes* which have an expected input and output type. 
2. You can only fit together nodes if the output type of the first node matches the input type of the second node.
3. Pipelines themselves can be thought of as "nodes" and can thus be composed to form new pipelines.
4. Wherever possible, nodes should take RDDs as input and produce them as output. This encourages node developers to think in data parallel terms.

This type-safe approach allows us to check that a pipeline will have a
reasonable chance of working at compile-time, leading to fewer issues when you
go to deploy your pipeline on hundreds of nodes.

The codebase you're working with today is a stripped-down preview of the larger
system we've built in the AMPLab. It provides a handful of nodes that fall into
the following categories:

1. Image Processing for data preprocessing and feature extraction.
2. General purpose statistical transformations for things like data normalization and scaling.
3. Linear solvers for training models.

Additionally, we've provided a number of utilities that are useful for things
like saving and loading pipelines once they've been trained, computing the
classification error of a trained pipeline, etc. The "complete" pipelines
repository is still a work-in-progress, but we're looking forward to sharing
it with the world when it's ready.


##Pipelines API

*Note this API can and will change before its public release - this is just a preview of the ideas behind it.*

The core of the pipelines API is very simple, and is inspired heavily by
type-safe functional programming. Here's the definition of a PipelineNode
and a Pipeline:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
object Pipelines {
  type PipelineNode[Input, Output] = (Input => Output)

  type Pipeline[Input, Output] = ((Input) => Output)
}
~~~
</div>
</div>

This might not make sense if you're new to Scala - but what this is saying is
that a "PipelineNode" has the same interface as a function from an input of
type "Input" to output of type "Output". That is - a pipeline node is *just* a
function.

To define a new *node*, you simply have to write a class that implements this
interface. Namely it has to have a method called `apply(x: Input): Output`. In
Scala, these "Input" and "Output" types are abstract.

Let's take a look at an example node:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
case object Vectorizer 
  extends PipelineNode[RDD[Image], RDD[Vector]]] 
  with Serializable {
  override def apply(in: RDD[Image]): RDD[Vector] = {
    in.map(_.toVector)
  }
}
~~~
</div>
</div>

This node simply takes an `RDD[Image]` as input, and produces an
`RDD[Vector]` as output. It does so by calling "_.toVector"
on each element of its input. This is obviously a very simple example.

What doing things this way buys us is the ability to use some features of the
Scala language to do things like compose pipelines using built in syntax.

For example - if I want to create a pipeline that consists of two nodes,
a vectorizer and a node that adds a "1" to the beginning of each vector,
I could just write:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val pipeline = Vectorizer andThen InterceptAdder //pipeline takes an RDD[Image] and returns an RDD[Vector] with 1 added to the front.

//To apply it to a dataset, I could just write:
val x: RDD[Image] = sc.objectFile(someFileName)
val result = pipeline.apply(x) //Result is an RDD[Vector]
//or equivalently
val result = pipeline(x) 
~~~
</div>
</div>

Note that this only works because the `InterceptAdder` node expects Vectors as
input.

Since the pipeline operations are really just transformations on RDD's, they
automatically get scheduled and executed efficiently by Spark. In the example
above, for instance, the pipeline won't even execute until an action is
performed on the result. Additionally, the map tasks between the two nodes will
automatically be pushed into a single task and executed together. 

In a moment, we'll see how these simple ideas let us perform complicated
machine learning tasks on distributed datasets.

##Data set

We'll be using a dataset of 60,000 images in 10 classes called
[CIFAR-10](http://www.cs.toronto.edu/~kriz/cifar.html). The techniques we'll
work with are designed to scale well to millions of images in thousands of
classes.

We'll be using the "binary" dataset from the CIFAR webpage, which is formatted
as follows:

~~~
<1 x label><3072 x pixel>
...
<1 x label><3072 x pixel>
~~~

But don't worry about matching pixel values to input data structures. We've
provided a standard data loader for data in this format that will take data and
represent it as an `Image`, which is the object type that all of the image
processing nodes in our pipelines will expect.

##Linear Classification

There are lots of different classification models out there - SVMs, Naive
Bayes, Decision Trees, Logistic Regression.
[MLlib](http://spark.apache.org/mllib) supports many of them. But today, we're
going to focus on *one* model family - specifically Linear Classification, and
instead see how proper *featurization* affects this choice of model.

Linear classifiers are the bread-and-butter of machine learning models. If
you've heard of linear regression - the concept of linear classification should
be pretty easy to understand. 

<p style="text-align: center;">
  <img src="img/linear-model.png"
       title="Linear Classification"
       alt="Linear Classification"
       width="75%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

Mathematically, we set up our "features" as a data matrix, `A`, of size (n x d)
where n is the number of training examples and d, the number of features from
some featurizer. The training labels, `b` are then a data matrix of size (n x
k) where each element is either `-1.0` if the training example belongs to a
particular class, or `+1.0` otherwise. A linear classifier learns a model, `x`,
of size (d x k) which minimizes the squared loss `|(Ax - b)^2|`. To control
overfitting we'll use a technique called regularization which adds a penalty
for models that aren't sparse.

There are several ways to solve linear models - including approximate methods
(e.g. gradient descent, coordinate descent) and exact methods (e.g. the normal
equations or QR decomposition).

As part of the pipelines project, we've developed several distributed solvers
for linear systems like this. For you - this means that you don't have to worry
about the details of how each of these work, and you can just call one to estimate
your model. We'll see how this works in a little bit.

##Setup

We will be using a standalone Scala project as a template for these exercises.
You will need to make sure you have the following file downloaded and unpacked 
on your drive somewhere:

  * On-site participants: [ampcamp-pipelines.zip](http://10.225.217.159/ampcamp-pipelines.zip)
  * Remote participants: [ampcamp-pipelines.zip](http://d12yw77jruda6f.cloudfront.net/ampcamp-pipelines.zip)

<div class="codetabs">

<div data-lang="scala">
<div class="prettyprint" style="margin-bottom:10px">
<ul style="margin-bottom:0px">
Unzip the file you downloaded for the pipelines project and navigate to the directory "ampcamp-pipelines"

You should find the following items in the directory:
<li><code>build.sbt</code>: SBT project file</li>
<li><code>LinearPixels.scala</code>: The simplest pipeline you're going to train and run.</li>
<li><code>RandomVocab.scala</code>: A better pipeline you're going to train and run.</li>
<li><code>PatchVocab.scala</code>: A reference for the better pipeline you're going to run.</li>
<li><code>data</code>: Directory containing "cifar_train.bin" and "cifar_test.bin"</li>
<li><code>src</code>: Directory containing the rest of the library.</li>
<li><code>saved_pipelines</code>: Directory containing some pre-built pipelines.</li>
<li><code>target</code>: Directory containing the packaged jar of this repository.</li>
</ul>
</div>
</div>
</div>

**IMPORTANT** Before going any further, make sure you have the `SPARK_HOME`
environment variable set in the terminal you're using to run the pipelines code.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#Make sure you do this in the root of the USB directory.
usb/$ export SPARK_HOME=[usb root directory]/spark
#On windows, use "set SPARK_HOME=[usb root directory]\spark"
</pre>
</div>
</div>


##A Simple Pipeline

As we've mentioned, we've provided data loaders for the CIFAR dataset.
The first, simplest pipeline we'll create attempts to use the pixel values
of the images to train an SVM model.

Let's take a look at the code for this pipeline: Locate the `LinearPixels`
class and open it with a text editor.

<div class="codetabs">

<div data-lang="scala">
<pre class="prettyprint lang-bsh">
usb/$ cd pipelines/scala
vim LinearPixels.scala  # Or your editor of choice
</pre>
</div>
</div>

The pipeline is defined fully here:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
package pipelines

import nodes._
import org.apache.spark.{SparkContext, SparkConf}
import utils.Stats

object LinearPixels {
  def main(args: Array[String]) = {
    val trainFile = args(0)
    val testFile = args(1)
    val conf = new SparkConf().setAppName("LinearPixels")
    val sc = new SparkContext(conf)
    val numClasses = 10

    //Define a node to load up our data.
    val dataLoader = new CifarParser() andThen new CachingNode()

    //Our training data is the result of applying this node to our input filename.
    val trainData = dataLoader(sc, trainFile)

    //A featurizer maps input images into vectors. For this pipeline, we'll also convert the image to grayscale.
    val featurizer = ImageExtractor andThen GrayScaler andThen Vectorizer
    val labelExtractor = LabelExtractor andThen ClassLabelIndicatorsFromIntLabels(numClasses) andThen new CachingNode

    //Our training features are the featurizer applied to our training data.
    val trainFeatures = featurizer(trainData)
    val trainLabels = labelExtractor(trainData)

    //We estimate our model as by calling a linear solver on our
    val model = LinearMapper.train(trainFeatures, trainLabels)

    //The final prediction pipeline is the composition of our featurizer and our model.
    //Since we end up using the results of the prediction twice, we'll add a caching node.
    val predictionPipeline = featurizer andThen model andThen new CachingNode

    //Calculate training error.
    val trainError = Stats.classificationError(predictionPipeline(trainData), trainLabels)

    //Do testing.
    val testData = dataLoader(sc, testFile)
    val testLabels = labelExtractor(testData)

    val testError = Stats.classificationError(predictionPipeline(testData), testLabels)

    EvaluateCifarPipeline.evaluatePipeline(testData, predictionPipeline, "linear_pixels")
    println(s"Training error is: $trainError, Test error is: $testError")

  }

}
~~~
</div>
</div>

This pipeline uses six nodes - a data loader, a label extractor, an image
extractor, a grayscale converter, a node to take the image pixels and flatten
them out into a vector for input to our linear solver, and a linear solver to
train a model on these pixels.

We call the collection of `ImageExtractor andThen GrayScaler andThen
Vectorizer` the featurizer - because it is what takes us from raw pixels to
input suitable for our linear classifier.

We've already built the code for you, so you can call this pipeline like so.
like so:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ cd ampcamp-pipelines

#If you use windows, change the forward slashes (/) below to backslash (\).
ampcamp-pipelines/$ bin/run-pipeline.sh pipelines.LinearPixels data/cifar_train.bin data/cifar_test.bin
</pre>
</div>
</div>

You'll see some intermediate output - and at the end, you'll see a line that
looks like this: 

    Training error is: 66.998, Test error is: 74.33
    
What does this mean? It means that on the test set, our simple pipeline
predicts the correct class ~25% of the time. Remember, there are 10 classes in
our dataset, and these classes are pretty evenly balanced, so while this model
is doing better than picking a class at random, it's still not great.

Let's visually verify that this isn't so great. The program has generated a
page of example classifications from the test set for you in the directory
`linear_pixels`. From the `ampcamp-pipelines` directory, open
`linear_pixels/index.html` in your web browser.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#On Windows - change the following command to open in your browser of choice.
ampcamp-pipelines/$ open linear_pixels/index.html
</pre>
</div>
</div>


##A Better Pipeline

So how do we do better than 75% error? The secret is in applying the concept of
a visual vocabulary which you heard about in the pipelines talk earlier. Switch
your editor to the file `RandomVocab.scala`. The main differences you'll see is
in how the featurizer is defined:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
val featurizer =
  ImageExtractor
    .andThen(new Convolver(sc, filterArray, imageSize, imageSize, numChannels, None, true))
    .andThen(SymmetricRectifier(alpha=alpha))
    .andThen(new Pooler(poolStride, poolSize, identity, _.sum))
    .andThen(new ImageVectorizer)
    .andThen(new CachingNode)
    .andThen(new FeatureNormalize)
    .andThen(new InterceptAdder)
    .andThen(new CachingNode)
~~~
</div>
</div>

Let's try running this code and seeing what it gives us. At the console

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
    #If you use windows, change the forward slashes (/) below to backslash (\).
    ampcamp-pipelines/$ bin/run-pipeline.sh pipelines.RandomVocab data/cifar_train.bin data/cifar_test.bin 200 10 0.2
</pre>
</div>
</div>

You'll notice there are three new arguments here. They are, the "size" of the
visual vocabulary to use (200), the regularization parameter (10), and the
fraction of the input data to train with. This last number is set to 20% here
in the interest of time.

You can see our featurizer has gotten a bit more complicated. In particular,
we've created a "filterArray" which is a bank of filters to be applied to the
input images. These filters have been generated *randomly* from a Gaussian
distribution. The filters represent our "visual vocabulary."

We then apply each of these 200 filters to the input image, and *pool* the
results into image quadrants. Different filters will react differently to each
filter in our "visual vocabulary." We then add a bias term, and use this term
as well as the pooled results as arguments to our linear classifier.

After a few minutes, your code will run and give you an answer similar to this:

    Training error is: 36.235596, Test error is: 42.88
    
Again, now let's look at the result visually, this time, the files are in the
`random_cifar` directory.

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#On Windows - change the following command to open in your browser of choice.
ampcamp-pipelines/$ open random_vocab/index.html
</pre>
</div>
</div>

###Advanced Exercise

If you have time, try changing around of of the parameters to the pipeline. For
example, try a different regularization value or number of filters (try 100 or
300). How does the accuracy change?


##An Advanced Pipeline

In the last pipeline, we used a "random" visual vocabulary, and while our
pipeline did work better than the simple pipeline on our data sample, getting
the category right leaves something to be desired.

The last key to this puzzle is using better "words" in our visual vocabulary.
For that, we'll use patch extraction and whitening.

Load up `PatchVocab.scala` to see what we mean.

Notice that the only real difference between this pipeline and the last one is
the following section has been added. 

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
    val patchExtractor = ImageExtractor
      .andThen(new Windower(patchSteps,patchSize))
      .andThen(new ImageVectorizer)
      .andThen(new Sampler(whitenerSize))

    val (filters, whitener) = {
        val baseFilters = patchExtractor(trainData)
        val baseFilterMat = Stats.normalizeRows(new MatrixType(baseFilters), 10.0)
        val whitener = new ZCAWhitener(baseFilterMat)

        //Normalize them.
        val sampleFilters = new MatrixType(Random.shuffle(baseFilterMat.toArray2.toList).toArray.slice(0, numFilters))
        val unnormFilters = whitener(sampleFilters)
        val twoNorms = MatrixFunctions.pow(MatrixFunctions.pow(unnormFilters, 2.0).rowSums, 0.5)

        (((unnormFilters divColumnVector (twoNorms.addi(1e-10))) mmul (whitener.whitener.transpose)).toArray2, whitener)
    }
~~~
</div>
</div>

Instead of being filters generated by the function `FloatMatrix.randn()`, our
filters are *sampled from the data.*

This is a powerful idea, because it means that instead of matching our images
to random noise (think static on a TV screen), we're matching images to how
much they look like things we recognize (like Mickey Mouse's ear or the logo of
the tenacious Philadelphia Eagles.)


##Loading A Pre-Trained Pipeline

Due to the computational requirements required to featurize the training data
and train the model on your machine in the time allotted, we've instead
provided a pre-trained pipeline with some extra bells and whistles.

We've shipped this pipeline to you as a "*.pipe" file, which is just a
serialized version of the same java object you've seen in the code.

The code to save, load, and apply a trained pipeline is very simple:

<div class="codetabs">
<div data-lang="scala" markdown="1">
~~~
import utils.PipelinePersistence._

//Save the pipeline.
savePipeline(predictionPipeline, "saved_pipelines/your_pipeline.pipe")
    
//Load the pipeline from disk.
val predictionPipeline = loadPipeline[RDD[Image],RDD[Array[DataType]]]("saved_pipelines/your_pipeline.pipe")
    
//Apply the prediction pipeline to new data.
val data: RDD[Image]
val predictions = predictionPipeline(data)    
~~~
</div>
</div>

As you'll see in a minute, we've also written a simple script to load up a
trained pipeline, evaluate it on a test dataset, and print out accuracy.

You'll notice that this makes the deployment strategy for pipelines very simple.
Once you're satisfied with the trained objects - ship them to a model serving
service like Velox, and you're good to go. This is by design.

##Pipeline Evaluation

So far, we've been focused on one number when evaluating our pipelines -
classification error. But, evaluating whether a classifier is good is more
nuanced than this. Consider the case of spam detection. As a user, you want
very few spam e-mails in your inbox, but you want to make *sure* that you don't
miss important messages. In this case, you're more sensitive to false negatives
(important messages that get filtered away) than false positives (a few spammy
e-mails showing up in your inbox).

So, to diagnose whether a spam prediction model is any good, we might look at
its contingency table.

<p style="text-align: center;">
  <img src="img/spam-ham.png"
       title="Contingency Table"
       alt="Contingency Table"
       width="50%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

We can generalize this contingency table to the multiclass setting, and we'll
see an example of that in a moment.

##Evaluate a Pipeline

To evaluate a previously constructed pipeline on test data, you'll need to do
the following:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#If you use windows, change the forward slashes (/) below to backslash (\).
ampcamp-pipelines/$ bin/run-pipeline.sh pipelines.EvaluateCifarPipeline saved_pipelines/patchvocab.pipe data/cifar_test.bin 0.2
<div class="solution">
#This is the full contingency table for this model - you will have fewer observation in your dataset.  
  
	plane	car	bird	cat	deer	dog	frog	horse	ship	truck
plane	    741	     26	     79	     24	     29	     18	      3	     22	     73	     33
car	     26	    791	     20	     16	      8	     13	      6	     10	     44	     64
bird	     37	     12	    511	     62	     56	     64	     50	     42	     13	     10
cat	     12	     16	     66	    456	     50	    143	     42	     29	     12	      7
deer	     19	      8	     81	     45	    582	     42	     47	     43	      8	      5
dog	      5	      5	     71	    187	     29	    608	     19	     59	      5	     10
frog	     18	     17	     94	    103	     99	     39	    807	     17	      6	     15
horse	     22	      8	     46	     47	     97	     42	     11	    749	      4	     11
ship	     80	     38	     17	     26	     20	     14	     10	      7	    801	     22
truck	     40	     79	     15	     34	     30	     17	      5	     22	     34	    823

Classification error on data/cifar_test.bin is: 31.31
</div>
</pre>
</div>
</div>

Part of your output should look like the "solution" above. The rows of this
table represent predictions of the model, while the columns are the true
classes. An entry (r,c) represents the number of times row r was predicted in
the test set and its actual class is c. Entries on the diagonal are good, and
large entries off the diagonal are points where the model got confused.

What's the highest non-diagonal entry? Does it make sense to you?

Finally, take a look at the `patch_cifar` visual output. Do you think the model
has gotten better?

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#On Windows - change the following command to open in your browser of choice.
ampcamp-pipelines/$ open patch_vocab/index.html
</pre>
</div>
</div>


#Advanced Exercise 2
There's another saved pipeline in your "saved_pipelines" folder. This one 
will take longer to evaluate, but gets even better error. The code that 
generated this pipeline is in `PatchVocab2.scala`

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
#If you use windows, change the forward slashes (/) below to backslash (\).
ampcamp-pipelines/$ bin/run-pipeline.sh pipelines.EvaluateCifarPipeline saved_pipelines/patchvocab2.pipe data/cifar_test.bin 0.2
</pre>
</div>
</div>

This pipeline was trained using a relatively large number of sample patches
(2000) and gives us about 24% test error. In order to train it, we had to make
a slightly more complicated pipeline that uses a boosting technique called 
block coordinate descent.

We know that if we scale up to 10,000
or so patches and pick the right regularization value, we can get to about 15%
error. This is horizontally scalable and competitive with recent
state-of-the-art academic results on this dataset.

###Advanced Exercise 3

Try actually changing a pipeline and recompiling it. You
can modify the pipelines in any text editor or IDE that supports scala, and
recompile with "sbt/sbt assembly." Try training a pipeline without adding an
intercept or whitening and see how that affects statistical performance.


##Concluding Remarks

You've now built and evaluated three sample pipelines for image classification
on Spark. We hope we've convinced you that this is a framework that's easy to
use and general purpose enough to capture your machine learning workflow. The
pipelines project is still in its infancy, so we're happy to receive feedback
now that you've used it.
