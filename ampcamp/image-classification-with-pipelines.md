---
layout: global
title: Image Classification with Pipelines
categories: [module]
navigation:
  weight: 80
  show: true
---

In this chapter, we'll use a preview of the ML pipelines framework to build an
image classifier. The goal of the application we're building is to take an
input image and automatically determine what is in it - e.g. a picture of a
bird should return the class "bird." While image classification is one
application that can be supported with the pipelines framework, we hope you'll
leave this exercise convinced that it will be useful for other tasks.

Before getting to the details of the classification task, let's quickly review
the goals of the project and the underlying principles of the framework.

##Getting Comfortable with the Pipelines Framework

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
4. Wherever possible, nodes should take RDDs as input and output. This encourages node developers to think in data parallel terms.

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

Linear classifiers are the bread-and-butter of machine learning models. If
you've heard of linear regression - the concept of linear classification should
be pretty easy to understand.

*TODO* - Put in an explanation of Ax = b and say that we want to solve for x.
Explain what's in the matrix. Picture of line fitting, and maybe show how it's
not that different from an SVM.

There are several ways to solve linear models - including approximate methods
(e.g. gradient descent, coordinate descent) and exact methods (e.g. the normal
equations or QR decomposition).

As part of the pipelines project, we've developed several distributed solvers
for linear systems like this. For you - this means that you don't have to worry
about the details of how each of these work, and you can just call one to solve
for your model. We'll see how this works in a little bit.

##Setup

We will be using a standalone scala project as a template for these exercises.

<div class="codetabs">

<div data-lang="scala">
<div class="prettyprint" style="margin-bottom:10px">
<ul style="margin-bottom:0px">
In the training USB drive, this has been setup in <code>pipelines/scala/</code>.<br>
You should find the following items in the directory:
<li><code>build.sbt</code>: SBT project file</li>
<li><code>LinearPixels.scala</code>: The simplest pipeline you're going to train and run.</li>
<li><code>RandomCifar.scala</code>: A better pipeline you're going to train and run.</li>
<li><code>RandomPatchCifar.scala</code>: A reference for the better pipeline you're going to run.</li>
<li><code>data</code>: Directory containing "cifar_train.bin" and "cifar_test.bin"</li>
<li><code>src</code>: Directory containing the rest of the library.</li>
</ul>
</div>
</div>
</div>

##A Simple Pipeline

As we've mentioned, we've provided data loaders for the CIFAR dataset.
The first, simplest pipeline we'll create attempts to use the pixel values
of the images to train an SVM model.

Let's take a look at the code for this pipeline: Let's first take a closer look
at our template code in a text editor, then we'll start adding code to the
template. Locate the `LinearPixels` class and open it with a text editor.

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
    val trainFile = args(1)
    val testFile = args(2)
    val sc = new SparkContext(args(0), "LinearPixels")
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

    println(s"Training error is: $trainError, Test error is: $testError")
  }

}
~~~
</div>
</div>

This pipeline uses five nodes - a data loader, a label extractor,
an image extractor, a node to take the image pixels and flatten them out into
a vector for input to our linear solver, and a linear solver to train a model
on these pixels.

We've already built the pipeline for you, so try running it on the input data like so:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
usb/$ cd pipelines/scala

scala/$ SPARK_MEM=8g bash run-main.sh pipelines.LinearPixels local[2] data/cifar_train.bin data/cifar_test.bin
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

##A Better Pipeline

So how do we do better than 75% error? The secret is in applying the concept of
a visual vocabulary which you heard about earlier. Switch your editor to the file
`RandomCifar.scala`. The main differences you'll see is in how the featurizer is
defined:

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
~~~
    scala/$ SPARK_MEM=8g bash run-main.sh pipelines.RandomCifar local[2] data/cifar_train.bin data/cifar_test.bin 200 10 0.2
~~~
</pre>
</div>
</div>

You'll notice there are three new arguments here. They are, the "size" of the
visual vocabulary to use (200), the regularization parameter (10), and the
fraction of the input data to train with. This last number is set to 20% here
in the interest of time.

While that's running - let's explain what's going on. In this example, we're
using exactly the same kind of model for our image classifier - a linear model.
Instead, all the hard work goes into "featurizing" our data.

The main featurization steps - convolution, rectification, pooling, and
normalization - can all be explained visually. ...

After a few minutes, your code will run and give you an answer.

    Training error is: 36.235596, Test error is: 42.88
    


##An Advanced Pipeline

In the last pipeline, we used a "random" visual vocabulary, and while our
pipeline did work better than the simple pipeline on our data sample, getting
the category right leaves something to be desired.

The last key to this puzzle is using better "words" in our visual vocabulary.
For that, we'll use patch extraction and whitening. 

*TODO* - Train a big one.


##Loading a pre-trained pipeline from disk.

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

##Pipeline Evaluation

So far, we've been focused on one number when evaluating our pipelines -
classification error. But, evaluating whether a classifier is good is more
nuanced than this. Consider the case of spam detection. As a user, you want
very few spam e-mails in your inbox, but you want to make *sure* that you don't
miss important messages. In this case, you're more sensitive to false negatives
(important messages that get filtered away) than false positives (a few spammy
e-mails showing up in your inbox).

So, to diagnose whether a spam prediction model is any good, we might look at
its precision/recall table.

*TODO* put down a sample P/R table here.

##Confusion Matrix

A confusion matrix is the multiclass analog of a simple precision/recall table.
It tells us, when we misclassify things, where do they go?

*TODO* code to generate confusion matrix.
Generate a confusion matrix here.


##Evaluate a Pipeline

To evaluate a previously constructed pipeline on test data, you'll need to do
the following:

<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
scala/$ SPARK_MEM=8g bash run-main.sh pipelines.EvaluateCifarPipeline local[2] saved_pipelines/randomPatchCifar.pipe data/cifar_test.bin
</pre>
</div>
</div>

You'll see some output that includes accuracy numbers along with a confusion matrix.

Here, we can see that the model most frequently confuses birds with airplanes. Are you surprised?

##Concluding Remarks

You've now built and evaluated three sample pipelines for image classification
on spark. We hope we've convinced you that this is a framework that's easy to
use and general purpose enough to capture your machine learning workflow. The
pipelines project is still in its infancy, so we're happy to receive feedback
now that you've used it.
