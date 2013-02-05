---
layout: global
title: Strata 2013 Exercises
---
# Introduction to Strata 2013 Exercises

The following series of exercises will walk you through the process of setting up a 4-machine cluster on EC2 running [Spark](http://spark-project.org), [Shark](http://shark.cs.berkeley.edu) and [Mesos](http://mesos-project.org),
then loading and analyzing a real wikipedia dataset using your cluster.
We will begin with simple interactive analysis techniques at the command-line using Spark and Shark, and progress to writing standalone programs using Spark and Spark Streaming, and then onto more advanced machine learning algorithms.
 
# Launching a Spark/Shark Cluster on EC2

This section will walk you through the process of launching a small cluster using your own Amazon EC2 account and our scripts and AMI (New to AMIs? See this [intro to AMIs](https://aws.amazon.com/amis/)).

## Pre-requisites

The cluster setup script we'll use below requires Python 2.x and has been tested to work on Linux or OS X.
We will use the [Bash shell](http://www.gnu.org/software/bash/manual/bashref.html) in our examples below.
If you are using Windows, consider installing [Cygwin](http://www.cygwin.com/) (note that we have not tested this, hence providing debug support would be hard).

## Setting up EC2 keys

Make sure you have an Amazon EC2 account.
Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your Amazon EC2 access key ID and secret access key.
These can be obtained from the [AWS homepage](http://aws.amazon.com/) by clicking `Account > Security Credentials > Access Credentials`:

![Downloading AWS access keys](img/aws-accesskey.png)

    export AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID>
    export AWS_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>

Create an Amazon EC2 key pair for yourself.
This can be done by logging into your Amazon Web Services account through the [AWS console](http://aws.amazon.com/console/), selecting `EC2` from the `Services` menu,  selecting `Key Pairs` on the left sidebar, and creating and downloading a key:

![Downloading an EC2 Keypair](img/aws-keypair.png)



Make sure that you set the permissions for the private key file to `600` (i.e. only you can read and write it) so that `ssh` will work (commands to do this are provided farther below).

<div class="alert alert-info">
<i class="icon-info-sign"> 	</i>
The AMI we are using for this exercise is only available in the `us-east` region.
So make sure you create a key-pair in that region!
</div>

## Getting the scripts to launch EC2 cluster

Check out the launch scripts by cloning the github repository.

    git clone git://github.com/amplab/ampcamp.git

You can also obtain them by downloading the zip file at `http://github.com/amplab/ampcamp/zipball/master`

## Launching the cluster
Launch the cluster by running the following command.
This script will launch a cluster, create a HDFS cluster and configure Mesos, Spark, and Shark.
Finally, it will copy the datasets used in the exercises from S3 to the HDFS cluster.
_This can take around 15-20 mins._

    cd ampcamp
    ./spark-ec2 -i <key_file> -k <name_of_key_pair> --copy launch ampcamp

Where `<name_of_key_pair>` is the name of your EC2 key pair (that you gave it when you created it), `<key_file>`is the private key file for your key pair.

For example, if you created a key pair named `ampcamp-key` and the private key (`<key_file>`) is in your home directory and is called `ampcamp.pem`, then the command would be

    ./spark-ec2 -i ~/ampcamp.pem -k ampcamp-key --copy launch ampcamp

The following are some errors that you may encounter, and other frequently asked questions:


<div class="accordion" id="q-accordion">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" href="#collapse-q1" data-parent="#q-accordion">
        I get an error when running this command: <code>UNPROTECTED KEY FILE...</code>
      </a>
      <a class="accordion-toggle" data-toggle="collapse" href="#collapse-q2" data-parent="#q-accordion">
        I get an error when running this command: <code>Your requested instance type (m2.xlarge) is not supported...</code>
      </a>
      <a class="accordion-toggle" data-toggle="collapse" href="#collapse-q3" data-parent="#q-accordion">
        I get an error when running this command: <code>java.lang.IllegalArgumentException: Invalid hostname in URI...</code>
      </a>
      <a class="accordion-toggle" data-toggle="collapse" href="#collapse-q4" data-parent="#q-accordion">
        Can I specify the instances types while creating the cluster?
      </a>
    </div>
    <div id="collapse-q1" class="accordion-body collapse">
      <div class="accordion-inner" markdown="1">
      
__Question: I got the following permission error when I ran the above command. Help!__

<pre class="nocode">
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@ WARNING: UNPROTECTED PRIVATE KEY FILE! @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Permissions 0644 for ‘../ampcamp.pem' are too open.
It is recommended that your private key files are NOT accessible by others.
This private key will be ignored.
bad permissions: ignore key: ../ampcamp.pem
Permission denied (publickey).
</pre>

__Answer:__ Run the next two commands.

    chmod 600 ../ampcamp.pem
    ./spark-ec2 -i <key_file> -k <name_of_key_pair> --copy --resume launch ampcamp



</div>
</div>
    <div id="collapse-q2" class="accordion-body collapse">
      <div class="accordion-inner" markdown="1">
      
__Question: I got the following permission error when I ran the above command. Help!__

<pre class="nocode">
"Your requested instance type (m2.xlarge) is not supported in your requested Availability Zone (us-east-1b).  Please retry your request by not specifying an Availability Zone or choosing us-east-1d, us-east-1c, us-east-1a, us-east-1e."
</pre>

__Answer:__ Add the `-z` flag to your command line arguments to use an availability zone other than `us-east-1b`.
You can set the value of that flag to "none", as in the following example command, which tells the script to pick a random availability zone.
It may randomly pick an availability zone that doesn't support this instance size (such as `us-east-1b`), so you may need to try this command a few times to get it to work.

    ./spark-ec2 -i <key_file> -k <name_of_key_pair> -z none --copy launch ampcamp

</div>
</div>
   <div id="collapse-q3" class="accordion-body collapse">
      <div class="accordion-inner" markdown="1">
      
__Question: I got the following error when I ran the above command. Help!__

<pre class="nocode">
12/08/21 16:50:45 INFO tools.DistCp: destPath=hdfs://ip-10-42-151-150.ec2.internal:9000/wiki/pagecounts
java.lang.IllegalArgumentException: Invalid hostname in URI
s3n://AKIAJIFGXUZ4MDJNYCGQ:COWo3AxVhjyu43Ug5kDvTnO/V3wQloBRIEOYEQgG@ak-ampcamp/wikistats_20090505-07
</pre>

__Answer:__ The data copy from S3 to your EC2 cluster has failed. Do the following steps:

1. Login to the master node by running

   ~~~
   ./spark-ec2 -i <key_file> -k <key_pair> login ampcamp
   ~~~

2. Open the HDFS config file at `/root/ephemeral-hdfs/conf/core-site.xml` and
   copy your AWS access key and secret key into the respective fields.

3. Restart HDFS

   ~~~
   /root/ephemeral-hdfs/bin/stop-dfs.sh
   /root/ephemeral-hdfs/bin/start-dfs.sh
   ~~~

4. Delete the directory the data was supposed to be copied to

   ~~~
   /root/ephemeral-hdfs/bin/hadoop fs -rmr /wiki
   ~~~

5. Logout and run the following command to retry copying data from S3

   ~~~
   ./spark-ec2 -i <key_file> -k <key_pair> copy-data ampcamp
   ~~~

</div>
</div>
   <div id="collapse-q4" class="accordion-body collapse">
      <div class="accordion-inner" markdown="1">
      
__Question: Can I specify the instances types while creating the cluster?__

__Answer:__ These exercises have been designed to work with at least 3 slave
machines using instances of type __m2.xlarge__.
You can also launch the cluster with different [instance types](http://aws.amazon.com/ec2/instance-types/).
However, you should ensure two things:

1. __Correct number of slaves:__ Make sure that the total memory in the slaves is about 54GB as the exercises are designed accordingly.
   So if you are using `m1.large` instances (which have 7.5 GB memory), then you should launch a cluster with at least 8 slaves.

   You can specify the instance type in the above command by setting the flag `-t <instance_type>` .
   Similarly, you can specify the number of slaves by setting the flag `-s <number of slaves>`.
   For example, to launching a cluster with 8 `m1.large` slaves, use

   ~~~
   ./spark-ec2 -i <key_file> -k <name_of_key_pair> -t m1.large -s 8 --copy launch ampcamp
   ~~~

2. __Correct java heap setting for Spark:__ Make sure to change the `SPARK_MEM` variable in
   `/root/spark/conf/spark-env.sh` and `/root/shark/conf/shark-env.sh` on all of the instances to match the amount of memory available in the instance type you use.
   This is typically set it to the total amount of memory of the instance minus 1 GB for the OS (that is, for `m1.large` with 7.5GB memory, set `SPARK_MEM=6g`).
   There is a easy way to change this configuration on all the instances.
   First, change this file in the master.
   Then run

   ~~~
   /root/mesos-ec2/copy-dir /root/spark/conf/ .
   ~~~

   to copy the configuration directory to all slaves.

__Information:__ Sometimes the EC2 instances don't initialize within the standard waiting time of 120 seconds.
If that happens you, will ssh errors (or check in the Amazon web console).
In this case, try increasing the waiting to 4 minutes using the `-w 240` option.

</div>
</div>

</div>
</div>

## Post-launch steps
Your cluster should be ready to use.
You can find the master hostname (`<master_node_hostname>` in the instructions below) by running

    ./spark-ec2 -i -k get-master ampcamp

At this point, it would be helpful to open a text file and copy `<master_node_hostname>` there.
In a later exercise, you will want to have `<master_node_hostname>` ready at hand without having to scroll through your terminal history.

## Terminating the cluster (Not yet! Only after you do the exercises below.)
__After you are done with your exercises__, you can terminate the cluster by running

    ./spark-ec2 -i <key_file> -k <key_pair> destroy ampcamp

# Logging into the Cluster

Log into your cluster via `ssh -i <key_file> -l root@<master_node_hostname>`

   __Question: I got the following permission error when I ran the above command. Help!__

   <pre class="nocode">
   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
   @             WARNING: UNPROTECTED PRIVATE KEY FILE!              @
   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
   Permissions 0644 for '../ampcamp.pem' are too open.
   It is recommended that your private key files are NOT accessible by others.
   This private key will be ignored.
   bad permissions: ignore key: ../ampcamp.pem
   Permission denied (publickey).
   </pre>

__Answer:__ Run the next two commands, then try to log in again:

    chmod 600 ../ampcamp.pem
    ./spark-ec2 -i <key_file> -k <name_of_key_pair> --copy --resume launch ampcamp


Once you are able to successfully launch the cluster, continue to step 4.

# Overview Of The Exercises
The exercises in this tutorial are divided into sections designed to give a hands-on experience with Spark, Shark and Spark Streaming. 
For Spark, we will walk you through using the Spark shell for interactive exploration of data. You have the choice of doing the exercises using Scala or using Python. 
For Shark, you will be using SQL in the Shark console to interactively explore the same data. 
For Spark Streaming, we will walk you through writing stand alone Spark programs in Scala to processing Twitter's sample stream of tweets. 
Finally, you will have to complete a complex machine learning exercise which will test your understanding of Spark. 

## Cluster Details
If you have launched the cluster with the default script above (no custom instance type and/or number of slaves), your cluster should contain 4 m2.xlarge Amazon EC2 nodes:

![Running EC2 instances in AWS Management Console](img/aws-runninginstances.png)

One of these 4 nodes is the master node, responsible for scheduling tasks as well as maintaining the HDFS metadata (a.k.a. HDFS name node).
The other 3 are the slave nodes on which tasks are actually executed.
You will mainly interact with the master node.
If you haven't already, let's ssh onto the master node:

    ssh -i <key_file> -l root <master_node_hostname>

On the cluster, run the `ls` command and you will see a number of directories.
Some of the more important ones are listed below:

- `ephemeral-hdfs:` Hadoop installation.
- `hive:` Hive installation
- `java-app-template:` Some stand-alone Spark programs in Java
- `mesos:` Mesos installation
- `mesos-ec2:` A suite of scripts to manage Mesos on EC2
- `scala-2.9.2.final:` Scala installation
- `scala-app-template:` Some stand-alone Spark programs in Scala 
- `spark:` Spark installation
- `shark:` Shark installation
- `streaming:` Stand-alone program for Spark Streaming exercises  

You can find a list of your 3 slave nodes in mesos-ec2/slaves:

    cat mesos-ec2/slaves

For stand-alone Spark programs, you will have to know the Spark cluster URL. You can find that in mesos-ec2/cluster-url:

    cat mesos-ec2/cluster-url

## Dataset For Exploration
Your HDFS cluster should come preloaded with 20GB of Wikipedia traffic statistics data obtained from http://aws.amazon.com/datasets/4182 .
To make the analysis feasible (within the short timeframe of the exercise), we took three days worth of data (May 5 to May 7, 2009; roughly 20G and 329 million entries).
You can list the files:

    ephemeral-hdfs/bin/hadoop fs -ls /wiki/pagecounts

There are 74 files (2 of which are intentionally left empty).

The data are partitioned by date and time.
Each file contains traffic statistics for all pages in a specific hour.
Let's take a look at the file:

    ephemeral-hdfs/bin/hadoop fs -cat /wiki/pagecounts/part-00148 | less

The first few lines of the file are copied here:

<pre class="nocode">
20090507-040000 aa ?page=http://www.stockphotosharing.com/Themes/Images/users_raw/id.txt 3 39267
20090507-040000 aa Main_Page 7 51309
20090507-040000 aa Special:Boardvote 1 11631
20090507-040000 aa Special:Imagelist 1 931
</pre>

Each line, delimited by a space, contains stats for one page.
The schema is:

`<date_time> <project_code> <page_title> <num_hits> <page_size>`

The `<date_time>` field specifies a date in the YYYYMMDD format (year, month, day) followed by a hyphen and then the hour in the HHmmSS format (hour, minute, second).
There is no information in mmSS.
The `<project_code>` field contains information about the language of the pages.
For example, project code "en" indicates an English page.
The `<page_title>` field gives the title of the Wikipedia page.
The `<num_hits>` field gives the number of page views in the hour-long time slot starting at `<data_time>`.
The `<page_size>` field gives the size in bytes of the Wikipedia page.

To quit `less`, stop viewing the file, and return to the command line, press `q`.

# Introduction to Scala
This short exercise will teach you the basics of Scala and introduce you to functional programming with collections.
Do as much as you feel you need. 

If you're comfortable with Python, feel free to skip ahead to the next section.

The exercise is based on a great and fast tutorial, [First Steps to Scala](http://www.artima.com/scalazine/articles/steps.html).
Just reading that and trying the examples at the console might be enough!
Open this in a separate browser window and look through it if you need help.
We will use only sections 1-9.

2. Launch the Scala console by typing `scala`.

3. Declare a list of integers, `numbers`, as `val numbers = List(1, 2, 5, 4, 7, 3)`

4. Declare a function, `cube`, that computes the cube (third power) of a number.
   See steps 2-4 of First Steps to Scala.
   Then apply the function on the list using `map`.

5. Then also try writing the function inline in a `map` call, using closure notation.

6. Define a `factorial` function that computes n! = 1 * 2 * ... * n given input n.
   You can use either a loop or recursion (see steps 5-7 of First Steps to Scala).
   Then compute the sum of factorials in `numbers`.

7. As a final exercise, implement a program that counts how many times a particular word occurs in a text file.
   You can load a text file as an array of lines as shown below:

   ~~~
   import scala.io.Source
   val lines = Source.fromFile("/root/mesos/README").getLines.toArray
   ~~~

   Then use functional methods to count the number of occurrences (steps 8-9 of First Steps to Scala).
   Also add a flag to your program to perform case-insensitive search.

# Data Exploration Using Spark

In this section, you are first going to use the Spark shell to interactively explore the Wikipedia data. Then we are going to give a bried introduction to writing standalone Spark programs.   

## Interactive Analysis

Let's now use Spark to do some order statistics on the data set.
First, launch the Spark shell:

<div class="codetabs">
  <div data-lang="scala" markdown="1">
    /root/spark/spark-shell
  </div>
  <div data-lang="python" markdown="1">
    /root/spark/pyspark
  </div>
</div>

Wait for the prompt to appear.

1. Warm up by creating an RDD (Resilient Distributed Dataset) named `pagecounts` from the input files.
   In the Spark shell, the SparkContext is already created for you as variable `sc`.

   <div class="codetabs">
     <div data-lang="scala" markdown="1">
       scala> sc
       res: spark.SparkContext = spark.SparkContext@470d1f30

       scala> val pagecounts = sc.textFile("/wiki/pagecounts")
       12/08/17 23:35:14 INFO mapred.FileInputFormat: Total input paths to process : 74
       pagecounts: spark.RDD[String] = spark.MappedRDD@34da7b85
     </div>
     <div data-lang="python" markdown="1">
       >>> sc
       <pyspark.context.SparkContext object at 0x7f7570783350>
       >>> pagecounts = sc.textFile("wiki/pagecounts")
       13/02/01 05:30:43 INFO mapred.FileInputFormat: Total input paths to process : 74
       >>> pagecounts
       <pyspark.rdd.RDD object at 0x217d510>
     </div>
   </div>

2. Let's take a peek at the data. You can use the take operation of an RDD to get the first K records. Here, K = 10.

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> pagecounts.take(10)
       ......
       res: Array[String] = Array(20090505-000000 aa.b ?71G4Bo1cAdWyg 1 14463, 20090505-000000 aa.b Special:Statistics 1 840, 20090505-000000 aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019, 20090505-000000 aa.b Wikibooks:About 1 15719, 20090505-000000 aa ?14mFX1ildVnBc 1 13205, 20090505-000000 aa ?53A%2FuYP3FfnKM 1 13207, 20090505-000000 aa ?93HqrnFc%2EiqRU 1 13199, 20090505-000000 aa ?95iZ%2Fjuimv31g 1 13201, 20090505-000000 aa File:Wikinews-logo.svg 1 8357, 20090505-000000 aa Main_Page 2 9980, 20090505-000000 aa User:TottyBot 1 7998, 20090505-000000 aa Wikipedia:Community_Portal 1 9644, 20090505-000000 aa main_page 1 928, 20090505-000000 ab.d %D0%A1%D0%BB%D1%83%D0%B6%D0%B5%D0%B1%D0%BD%D0%B0%D1%8F: \
       Recentchangeslinked/%D0%A3%D1%87%D0%B0%D1%81%D1%82%D0%BD%D0%B8%D0%BA: \
       H%C3%A9g%C3%A9sippe_...
   </div>
   <div data-lang="python" markdown="1">
       >>> pagecounts.take(10)
       ...
       [u'20090505-000000 aa.b ?71G4Bo1cAdWyg 1 14463', u'20090505-000000 aa.b Special:Statistics 1 840', u'20090505-000000 aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019', u'20090505-000000 aa.b Wikibooks:About 1 15719', u'20090505-000000 aa ?14mFX1ildVnBc 1 13205', u'20090505-000000 aa ?53A%2FuYP3FfnKM 1 13207', u'20090505-000000 aa ?93HqrnFc%2EiqRU 1 13199', u'20090505-000000 aa ?95iZ%2Fjuimv31g 1 13201', u'20090505-000000 aa File:Wikinews-logo.svg 1 8357', u'20090505-000000 aa Main_Page 2 9980']
   </div>
   </div>

   Unfortunately this is not very readable because take returns an array and Scala simply prints the array with each element separated by a comma.
   We can make it prettier by traversing the array to print each record on its own line.

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> pagecounts.take(10).foreach(println)
       ......
       20090505-000000 aa.b ?71G4Bo1cAdWyg 1 14463
       20090505-000000 aa.b Special:Statistics 1 840
       20090505-000000 aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019
       20090505-000000 aa.b Wikibooks:About 1 15719
       20090505-000000 aa ?14mFX1ildVnBc 1 13205
       20090505-000000 aa ?53A%2FuYP3FfnKM 1 13207
       20090505-000000 aa ?93HqrnFc%2EiqRU 1 13199
       20090505-000000 aa ?95iZ%2Fjuimv31g 1 13201
       20090505-000000 aa File:Wikinews-logo.svg 1 8357
       20090505-000000 aa Main_Page 2 9980
   </div>
   <div data-lang="python" markdown="1">
       >>> for x in pagecounts.take(10):
       ...    print x
       ...
       20090505-000000 aa.b ?71G4Bo1cAdWyg 1 14463
       20090505-000000 aa.b Special:Statistics 1 840
       20090505-000000 aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019
       20090505-000000 aa.b Wikibooks:About 1 15719
       20090505-000000 aa ?14mFX1ildVnBc 1 13205
       20090505-000000 aa ?53A%2FuYP3FfnKM 1 13207
       20090505-000000 aa ?93HqrnFc%2EiqRU 1 13199
       20090505-000000 aa ?95iZ%2Fjuimv31g 1 13201
       20090505-000000 aa File:Wikinews-logo.svg 1 8357
       20090505-000000 aa Main_Page 2 9980
   </div>
   </div>

2. Let's see how many records in total are in this data set (this command will take a while, so read ahead while it is running).

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> pagecounts.count
   </div>
   <div data-lang="python" markdown="1">
       >>> pagecounts.count()
   </div>
   </div>

   This should launch 177 Spark tasks on the Mesos cluster.
   If you look closely at the terminal, the console log is pretty chatty and tells you the progress of the tasks.
   Because we are reading 20G of data from HDFS, this task is I/O bound and can take a while to scan through all the data (2 - 3 mins).
￼
   While it's running, you can open the Mesos web console to see the progress.
   To do this, open your favorite browser, and type in the following URL.
   Recall that during the Cluster Setup section, you copied `<master_node_hostname>` to a text file for easy access.

   `http://<master_node_hostname>:8080`

   ![Mesos web UI](img/mesos-webui640.png)

   When your count does finish running, it should return the following result: `res2: Long = 329641466`

4. Recall from above when we described the format of the data set, that the second field is the "project code" and contains information about the language of the pages.
   For example, the project code "en" indicates an English page.
   Let's derive an RDD containing only English pages from `pagecounts`.
   This can be done by applying a filter function to `pagecounts`.
   For each record, we can split it by the field delimiter (i.e. a space) and get the second field-– and then compare it with the string "en".

   To avoid reading from disks each time we perform any operations on the RDD, we also __cache the RDD into memory__.
    This is where Spark really starts to to shine.

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> val enPages = pagecounts.filter(_.split(" ")(1) == "en").cache
       enPages: spark.RDD[String] = spark.FilteredRDD@8262adc
   </div>
   <div data-lang="python" markdown="1">
       >>> enPages = pagecounts.filter(lambda x: x.split(" ")[1] == "en").cache()
   </div>
   </div>

   When you type this command into the Spark shell, Spark defines the RDD, but because of lazy evaluation, no computation is done yet.
   Next time any action is invoked on `enPages`, Spark will cache the data set in memory across the 3 slaves in your cluster.

5. How many records are there for English pages?

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> enPages.count
       ......
       res: Long = 122352588
   </div>
   <div data-lang="python" markdown="1">
       >>> enPages.count()
       ...
       122352588
   </div>
   </div>

   The first time this command is run, similar to the last count we did, it will take 2 - 3 minutes while Spark scans through the entire data set on disk.
   __But since enPages was marked as "cached" in the previous step, if you run count on the same RDD again, it should return an order of magnitude faster__.

   If you examine the console log closely, you will see lines like this, indicating some data was added to the cache:

   `12/08/18 00:30:55 INFO spark.CacheTrackerActor: Cache entry added: (2, 0) on ip-10-92-69-90.ec2.internal (size added: 16.0B, available: 4.2GB)`

6. Let's try something fancier.
   Generate a histogram of total page views on Wikipedia English pages for May to May 7, 2009.
   The high level idea of what we'll be doing is as follows.
   First, we generate a key value pair for each line; the key is the date (the first eight characters of the first field), and the value is the number of pageviews for that date (the fourth field).

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> val enTuples = enPages.map(line => line.split(" "))
       enTuples: spark.RDD[Array[java.lang.String]] = spark.MappedRDD@5a62a404

       scala> val enKeyValuePairs = enTuples.map(line => (line(0).substring(0, 8), line(3).toInt))
       enKeyValuePairs: spark.RDD[(java.lang.String, Int)] = spark.MappedRDD@142eda55
   </div>
   <div data-lang="python" markdown="1">
       >>> enTuples = enPages.map(lambda x: x.split(" "))
       >>> enKeyValuePairs = enTuples.map(lambda x: (x[0][:8], int(x[3])))
   </div>
   </div>

   Next, we shuffle the data and group all values of the same key together.
   Finally we sum up the values for each key.
   There is a convenient method called `reduceByKey` in Spark for exactly this pattern.
   Note that the second argument to `reduceByKey` determines the number of reducers to use.
   By default, Spark assumes that the reduce function is algebraic and applies combiners on the mapper side.
   Since we know there is a very limited number of keys in this case (because there are only 3 unique dates in our data set), let's use only one reducer.

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> enKeyValuePairs.reduceByKey(_+_, 1).collect
       ......
       res: Array[(java.lang.String, Int)] = Array((20090506,204190442), (20090507,202617618), (20090505,207698578))
   </div>
   <div data-lang="python" markdown="1">
       >>> enKeyValuePairs.reduceByKey(lambda x, y: x + y, 1).collect()
       ...
       [(u'20090506', 204190442), (u'20090507', 202617618), (u'20090505', 207698578)]
   </div>
   </div>

   The `collect` method at the end converts the result from an RDD to a Scala array.
   Note that when we don't specify a name for the result of a command (e.g. `val enTuples` above), a variable with name `res` is automatically created.

   We can combine the previous three commands into one:

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> enPages.map(line => line.split(" ")).map(line => (line(0).substring(0, 8), line(3).toInt)).reduceByKey(_+_, 1).collect
       ......
       res: Array[(java.lang.String, Int)] = Array((20090506,204190442), (20090507,202617618), (20090505,207698578))
   </div>
   <div data-lang="python" markdown="1">
       >>> enPages.map(lambda x: x.split(" ")).map(lambda x: (x[0][:8], int(x[3]))).reduceByKey(lambda x, y: x + y, 1).collect()
       ...
       [(u'20090506', 204190442), (u'20090507', 202617618), (u'20090505', 207698578)]
   </div>
   </div>

7. Suppose we want to find the top 50 most-viewed pages during these three days.
   Conceptually, this task is very similar to the previous query.
   But, given the large number of pages (23 million distinct page names), the new task is very expensive.
   We are doing a super expensive group-by with a lot of network shuffling of data.

   Also, we would like to use the distributed sorting in Spark rather than loading all 23 million results to the master node to take the top K.
   Spark provides a `sortByKey` method for an RDD, but the value needs to come before the key in a tuple when running this method.
   So make sure you swap the key and value in each tuple that results from `reduceByKey` before running `sortByKey`.

   To recap, first we find the fields in each line of data (`map(l => l.split(" "))`).
   Next, we extract the fields for page name and number of page views (`map(l => (l(2), l(3).toInt))`).
   We reduce by key again, this time with 40 reducers (`reduceByKey(_+_, 40)`).
   Finally, we swap the key and values (`map(x => (x._2, x._1))`), sort by key in descending order (the `false` argument specifies descending order and `true` would specify ascending), and return the top 50 results (`take`). The full command is:

   <div class="codetabs">
   <div data-lang="scala" markdown="1">
       scala> enPages.map(l => l.split(" ")).map(l => (l(2), l(3).toInt)).reduceByKey(_+_, 40).map(x => (x._2, x._1)).sortByKey(false).take(50)
   </div>
   <div data-lang="python" markdown="1">
       >>> # TODO: sortByKey() isn't implemented in PySpark yet.
       >>> enPages.map(lambda x: x.split(" ")).map(lambda x: (x[2], int(x[3]))).reduceByKey(lambda x, y: x + y, 40).map(lambda x: (x[1], x[0])).sortByKey(false).take(50)
       TODO
   </div>
   </div>

   There is no hard and fast way to calculate the optimal number of reducers for a given problem; you will
   build up intuition over time by experimenting with different values.

   To leave the Spark shell, type `exit` at the prompt.


## Introduction To Running Standalone Programs

So far we have been doing a lot of ad-hoc style analytics using the command line interfaces to Spark and Shark. For some tasks, it makes more sense to write a standalone Spark program. 
On our AMI we have included two "template" projects for Scala and Java standalone Spark programs. 
These can be found in the `/root/scala-app-template` and `/root/java-app-template` directories. 
These projects are built using [Simple Built Tool (SBT)](http://www.scala-sbt.org/), a popular scala build system. 
You can try to compile your application into a JAR using  

~~~
sbt/sbt package
~~~

Then, you can run your application with command line parameters using 

~~~
sbt/sbt run <arg1> <arg2> 
~~~

Feel free to browse through the contents of those directories. For more details, see the slides from Matei Zaharia's AMP Camp talk on Standalone Spark Programs which are linked to from [the AMP Camp Agenda page](http://ampcamp.berkeley.edu/agenda).


# Data Exploration Using Shark

Now that we've had fun with Scala, let's try some SQL in Shark.
First, launch the Shark console:

    cd /root/
    /root/shark/bin/shark-withinfo

1. Similar to [2Apache Hive, Shark can query external tables (tables that are not created in Shark).
   Before you do any querying, you will need to tell Shark where the data is and define its schema.

   ~~~
   shark> CREATE EXTERNAL TABLE wikistats (dt string, project_code string, page_name string, page_views int, bytes int) row format delimited fields terminated by ' ' location '/wiki/pagecounts';
   ~~~

   FAQ: If you see the following errors, don’t worry. Things are still working under the hood.

   <pre class="nocode">
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.core.resources" but it cannot be resolved.
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.core.runtime" but it cannot be resolved.
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.text" but it cannot be resolved.
   </pre>

2. Let's create a table containing all English records and cache it in the cluster's memory.

   ~~~
   shark> create table wikistats_cached as select * from wikistats where project_code="en";
   ~~~

3. Do a simple count to get the number of English records. If you have some familiarity working with databases, note that "`count(1)`" is the syntax in Hive for performing a count(*) operation. The Hive syntax is described at https://cwiki.apache.org/confluence/display/Hive/GettingStarted

   ~~~
   shark> select count(1) from wikistats_cached;
   ......
   122352588
   Time taken: 7.632 seconds
   12/08/18 21:23:13 INFO CliDriver: Time taken: 7.632 seconds
   ~~~

4. Output the total traffic to Wikipedia English pages for each hour between May 7 and May 9, with one line per hour.

   ~~~
   shark> select dt, sum(page_views) views from wikistats_cached group by dt;
   ~~~

5. In the Spark section, we ran a very expensive query to compute the top 50 pages. It is fairly simple to d the same thing in SQL.

   There are two steps we can take to make the query run faster. (1) In the first command below, we turn off map-side aggregation (a.k.a. combiners). Map-side aggregation doesn't improve run-time since each page appears only once in each partition, so we are building a giant hash table on each partition for nothing.
(2) In the second command below, we increase the number of reducers used in this query to 50. Note that the
default number of reducers, which we have been using so far in this section, is 1.

   Also note that the "50" in the second command refers to the number of reducers while the "50" in the third command refers to the number of Wikipedia pages to return.

   ~~~
   shark> set hive.map.aggr=false;
   shark> set mapred.reduce.tasks=50;
   shark> select page_name, sum(page_views) views from wikistats_cached group by page_name order by views desc limit 50;
   ~~~

   __Remember to turn hive.map.aggr back on after the query.__

6. With all the warm up, now it is your turn to write queries. Write Hive QL queries to answer the following questions:

- Count the number of distinct date/times for English pages

   <div class="solution" markdown="1">
      select count(distinct dt) from wikistats_cached;
   </div>

- How many hits are there on pages with Berkeley in the title throughout the entire period?

   <div class="solution" markdown="1">
      select count(page_views) from wikistats_cached where page_name like "%berkeley%";
   </div>

- Generate a histogram for the number of hits for each hour on May 6, 2009; sort the output by date/time. Based on the output, which hour is Wikipedia most popular?

   <div class="solution" markdown="1">
      select dt, sum(page_views) from wikistats_cached where dt like "20090506%" group by dt order by dt;
   </div>

To exit Shark, type the following at the Shark command line (and don't forget the semicolon!).

    shark> exit;


# Processing Live Data Streams with Spark Streaming

In this section, we will walk you through using Spark Streaming to process live data streams. These exercises are designed as standalone Scala programs which will receive and process Twitter's sample tweet streams. If you are not familiar with Scala, it is recommended that you see the [Intro to Scala](#intro-to-scala) section to familiarize yourself with the language.

## Setup 
We use a modified version of the Scala standalone project template introduced in the [Intro to Running Standalone Programs](#introduction-to-running-standalone-spark-programs) section for the next exercise. In your AMI, this has been setup in `/root/streaming/`. You should find the following items in the directory.

- `build.sbt:` SBT project file
- `login.txt:` File containing Twitter username and password 
- `sbt:` Directory containing the SBT tool
- `Tutorial.scala:` Main Scala program that you are going to edit, compile and run
- `TutorialHelper.scala:` Scala file containing few helper functions for `Tutorial.scala`

The main file you are going to edit, compile and run for the exercises is the `Tutorial.scala`. The file should the following.

~~~
import spark._
import spark.streaming._
import StreamingContext._
import TutorialHelper._

object Tutorial {
  def main(args: Array[String]) {
    
    // Location of the Spark directory 
    val sparkHome = "/root/spark"
    
    // URL of the Spark cluster
    val sparkUrl = getSparkUrl()

    // Location of the required JAR files 
    val jarFile = "target/scala-2.9.2/tutorial_2.9.2-0.1-SNAPSHOT.jar"

    // HDFS directory for checkpointing
    val checkpointDir = TutorialHelper.getHdfsUrl() + "/checkpoint/"

    // Twitter credentials from login.txt
    val (twitterUsername, twitterPassword) = getTwitterCredentials()
   
    // Your code goes here
  }
}
~~~

For your convenience, we have added a couple of helper function to get the parameters that the exercises need.

- `getSparkUrl()` is a helper function that fetches the Spark cluster URL from the file `/root/mesos-ec2/cluster-url`. 
- `getTwitterCredential()` is another helper function that fetches the Twitter username and password from the file `/root/streaming/login.txt`. 

Since all the exercises are based on Twitter's sample tweet stream, they require you specify a Twitter account's username and password. You can either use you your own Twitter username and password, or use one of the few account we made for the purpose of this tutorial. The username and password needs to be set in the file `/root/streaming/login.txt`

<pre class="nocode">
my.fancy.username
my_uncrackable_password
</pre>

Be sure to delete this file after the exercises are over. Even if you don't delete them, these files will be completely destroyed along with the instance, so your password will not fall into wrong hands. 


## First Spark Streaming program
Let's try to write a very simple Spark Streaming program that prints a sample of the tweets it receives from Twitter every second. 
We will edit the file `Tutorial.scala` in the directory `/root/streaming/`

<pre>
cd /root/streaming/
vim Tutorial.scala
</pre>

You can use either vim or emacs for editing. Alternatively, you can use your favorite text editor to write your program and then copy-paste it to the file using vim or emacs before running it.


To express any Spark Streaming computation, a StreamingContext object needs to be created. 
This object serves as the main entry point for all Spark Streaming functionality.

~~~
    val sc = new SparkContext(sparkUrl, "Tutorial", sparkHome, Seq(jarFile))
    val ssc = new StreamingContext(sc, Seconds(1))
~~~

Here, a SparkContext object is first created by providing the Spark cluster URL, the Spark home directory and the list of JAR files that are necessary to run the program. 
"Tutorial" is a unique name given to this application to identify it the Spark's web UI.
Using this SparkContext object, a StreamingContext object is created. `Seconds(1)` tells the context to receive and process data in batches of 1 second. 
Next, we use this context and the login information to create a stream of tweets.

~~~
    val tweets = ssc.twitterStream(twitterUsername, twitterPassword)
~~~

The object `tweets` is a DStream of tweet statuses. More specifically, it is continuous stream of RDDs containing objects of type [twitter4j.Status](http://twitter4j.org/javadoc/twitter4j/Status.html). As a very simple processing step, let's try to print the status text of the some of the tweets. 

~~~
    val statuses = tweets.map(status => status.getText())
    statuses.print()
~~~

Similar to RDD transformation in the earlier Spark exercises, the `map` operation on `tweets` maps each Status object to its text to create a new 'transformed' DStream named `statuses`. The `print` output operation tells the context to print first 10 records in each RDD in a DStream, which in this case, are 1 second batches of received status texts. 

We also need to set an HDFS for periodic checkpointing of the intermediate data. 

~~~
    ssc.checkpoint(checkpointDir)
~~~
Finally, we need to tell the context to start running the computation we have setup. 

~~~
    ssc.start()
~~~

__Note that all DStream operations must be done before calling this statement.__

After saving `Tutorial.scala`, it can be run from the command prompt using the following command (from within the `/root/streaming` directory).

~~~
sbt/sbt project run
~~~

This command will automatically compile `Tutorial.scala` to create a JAR file in `/root/streaming/target/scala-2.9.2/` and then run the program. You should find the following output on your screen.

<pre class="nocode">
-------------------------------------------
Time: 1359886325000 ms
-------------------------------------------
RT @__PiscesBabyyy: You Dont Wanna Hurt Me But Your Constantly Doing It
@Shu_Inukai ?????????????????????????????????????????
@Condormoda Us vaig descobrir a la @080_bcn_fashion. Molt bona desfilada. Salutacions des de #Manresa
RT @dragon_itou: ?RT???????3000???????????????????????????????????10???????

?????????????????2?3???9???? #???? http://t.co/PwyA5dsI ? h ...
Sini aku antar ke RSJ ya "@NiieSiiRenii: Memang (?? ?`? )"@RiskiMaris: Stresss"@NiieSiiRenii: Sukasuka aku donk:p"@RiskiMaris: Makanya jgn"
@brennn_star lol I would love to come back, you seem pretty cool! I just dont know if I could ever do graveyard again :( It KILLs me
????????????????????????????????????????????????????????????????????????????????????????ww
??????????
When the first boats left the rock with the artificers employed on.
@tgs_nth ????????????????????????????
...


-------------------------------------------
Time: 1359886326000 ms
-------------------------------------------
???????????
???????????
@amatuki007 ????????????????????????????????
?????????????????
RT @BrunoMars: Wooh!
Lo malo es qe no tiene toallitas
Sayang beb RT @enjaaangg Piye ya perasaanmu nyg aku :o
Baz? ?eyler yar??ma ya da reklam konusu olmamal? d???ncesini yenemiyorum.
?????????????MTV???????the HIATUS??
@anisyifaa haha. Cukupla merepek sikit2 :3
@RemyBot ?????????
...
</pre>

To stop the application, use `Ctrl + c` . Instead of this, if you see the following on your screen, it means that the authentication with Twitter failed. 

<pre class="nocode">
13/02/04 23:41:57 INFO streaming.NetworkInputTracker: De-registered receiver for network stream 0 with message 401:Authentication credentials (https://dev.twitter.com/pages/auth) were missing or incorrect. Ensure that you have set valid consumer key/secret, access token/secret, and the system clock is in sync.
&lt;html&gt;
&lt;head&gt;
&lt;meta http-equiv="Content-Type" content="text/html; charset=utf-8"/&gt;
&lt;title&gt;Error 401 Unauthorized&lt;/title&gt;
&lt;/head&gt;
&lt;body&gt;
&lt;h2&gt;HTTP ERROR: 401&lt;/h2&gt;
&lt;p&gt;Problem accessing '/1.1/statuses/sample.json?stall_warnings=true'. Reason:
&lt;pre&gt;    Unauthorized&lt;/pre&gt;



&lt;/body&gt;
&lt;/html&gt;


Relevant discussions can be found on the Internet at:
	http://www.google.co.jp/search?q=d0031b0b or
	http://www.google.co.jp/search?q=1db75513
TwitterException{exceptionCode=[d0031b0b-1db75513], statusCode=401, message=null, code=-1, retryAfter=-1, rateLimitStatus=null, version=3.0.3}
</pre>

Please verify whether the Twitter username and password has been set correctly in the file `login.txt` as instructed earlier. Make sure you do not have unnecessary trailing spaces. 



## Further exercises
Next, let's try something more interesting, say, try printing the 10 most popular hashtags in the last 30 seconds. These next steps explain the set of the DStream operations required to achieve our goal. As mentioned before, the operations explained in the next steps must be added in the program before `ssc.start()`. After every step, you can see the contents of new DStream you created by using the `print()` operation and running Tutorial in the same way as explained earlier (that is, `sbt/sbt package run`).

1. __Get the stream of hashtags from the stream of tweets__ : 
   To get the hashtags from the status string, we need to identify only those words in the message that start with "#". This can be done as follows.
    
   ~~~
      val words = statuses.flatMap(status => status.split(" "))
      val hashtags = words.filter(word => word.startsWith("#"))
   ~~~

   The `flatMap` operation applies a one-to-many operation to each record in a DStream and then flattens the records to create a new DStream. 
   In this case, each status string is split by space to produce a DStream whose each record is a word. 
   Then we apply the `filter` function to retain only the hashtags. The resulting `hashtags` DStream is a stream of RDDs having only the hashtags.
   If you want to see the result, add `hashtags.print()` and try running the program. 
   You should see something like this (assuming no other DStream has `print` on it).

   <pre class="nocode">
   -------------------------------------------
   Time: 1359886521000 ms
   -------------------------------------------
   #njnbg
   #njpw
   #?????
   #algeria
   #Annaba

   </pre>

2. __Count the hashtags over a window 30 seconds__ : Next, these hashtags need to be counted over a window.  
  This can be done by first mapping 

   ~~~
      val counts = hashtags.map(t => (t, 1))
                           .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(1))
   ~~~

   The generated `counts` DStream will have records that are (hashtag, count) tuples.
   If you `print` counts and run this program, you should see something like this. 

   <pre class="nocode">
   -------------------------------------------
   Time: 1359886694000 ms
   -------------------------------------------
   (#epic,1)
   (#WOWSetanYangTerbaik,1)
   (#recharged,1)
   (#??????????,1)
   (#jaco,1)
   (#Blondie,1)
   (#TOKIO,1)
   (#fili,1)
   (#jackiechanisamazing,1)
   (#DASH,1)
   ...
   </pre>


3. __Find the top 10 hashtags based on their counts__ : 
   Finally, these counts have to be used to find the popular hashtags. 
   A simple (but not the most efficient) way to do this is to sort the hashtags based on their counts and
   take the top 10 records. Since this requires sorting by the counts, the count (i.e., the second item in the 
   (hashtag, count) tuple) needs to be made the key. Hence, we need to first use a `map` to flip the tuple and 
   then sort the hashtags. Finally, we need to get the top 10 hashtags and print them. All this can be done as follows.

   ~~~
       val sortedCounts = counts.map { case(tag, count) => (count, tag) }
                                .transform(rdd => rdd.sortByKey(false))
       sortedCounts.foreach(rdd => 
         println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))
   ~~~

   The `transform` operation allows any arbitrary RDD-to-RDD operation to be applied to each RDD of a DStream to generate a new DStream. 
   As the name suggests, `sortByKey` is an RDD operation that does a distributed sort on the data in the RDD (`false` to ensure descending order). 
   The resulting 'sortedCounts' DStream is a stream of RDDs having sorted hashtags. 
   The `foreach` operation applies a given function on each RDD in a DStream, that is, on each batch of data. In this case, 
   `foreach` is used to get the first 10 hashtags from each RDD in `sortedCounts` and print them, every second.  
   If you run this program, you should see something like this. 

   <pre class="nocode">
   Top 10 hashtags:
   (2,#buzzer)
   (1,#LawsonComp)
   (1,#wizkidleftEMEcos)
   (1,#???????)
   (1,#NEVERSHUTMEUP)
   (1,#reseteo.)
   (1,#casisomoslamismapersona)
   (1,#job)
   (1,#????_??_?????_??????)
   (1,#?????RT(*^^*))
   </pre>

   Note that there are more efficient ways to get the top 10 hashtags. For example, instead of sorting the entire of 
   30-second-counts (thereby, incurring the cost of a data shuffle), one can get the top 10 hashtags in each partition, 
   collect them together at the driver and then find the top 10 hashtags among them.
   We leave this as an exercise for the reader to try out. 


# Machine Learning

To allow you to complete the machine learning exercises within the relatively short time available, using only the relatively small number of nodes available to you, we will now work with a restricted set of the Wikipedia traffic statistics data from May 5-7, 2009. In particular, we have restricted the dataset to only include a subset of the full set of articles. This restricted dataset is pre- loaded in the HDFS on your cluster in `/wikistats_20090505-07_restricted`.


## Command Line Preprocessing and Featurization

To apply most machine learning algorithms, we first must preprocess and featurize the data.  That is, for each data point, we must generate a vector of numbers describing the salient properties of that data point.  In our case, each data point will consist of a unique Wikipedia article identifier (i.e., a unique combination of Wikipedia project code and page title) and associated traffic statistics.  We will generate 24-dimensional feature vectors, with each feature vector entry summarizing the page view counts for the corresponding hour of the day.

Recall that each record in our dataset consists of a string with the format "`<date_time> <project_code> <page_title> <num_hits> <page_size>`".  The format of the date-time field is YYYYMMDD-HHmmSS (where 'M' denotes month, and 'm' denotes minute).

1. You can preprocess and featurize the data yourself by following along with the parts of this exercise. To skip ahead to exercise 2 where we start examining the data set we will input to K means, just copy and paste all of the code from our solution to preprocess the data.

   <div class="solution" markdown="1">

   Place the following code within a Scala `object` and call the `featurization` function from a `main` function:

   ~~~
   import scala.io.Source
   import spark.SparkContext
   import SparkContext._
   lazy val hostname = Source.fromFile("/root/mesos-ec2/masters").mkString.trim
   def featurization(sc: SparkContext) {
    val featurizedRdd = sc.textFile("hdfs://"+hostname+":9000/wikistats_20090505-07_restricted").map{line => {
      val Array(dateTime, projectCode, pageTitle, numViews, numBytes) = line.trim.split("\\s+")
      val hour = dateTime.substring(dateTime.indexOf("-")+1, dateTime.indexOf("-")+3).toInt
      (projectCode+" "+pageTitle, hour -> numViews.toInt)
    }}.groupByKey.map{grouped => {
      val (article, hoursViews) = grouped
      val sums = Array.fill[Int](24)(0)
      val counts = Array.fill[Int](24)(0)
      for((hour, numViews) <- hoursViews) {
        sums(hour) += numViews
        counts(hour) += 1
      }
      val avgs: Array[Double] =
        for((sum, count) <- sums zip counts) yield
          if(count > 0) sum/count.toDouble else 0.0
      article -> avgs
    }}.filter{t => {
      t._2.forall(_ > 0)
    }}.map{t => {
      val avgsTotal = t._2.sum
      t._1 -> t._2.map(_ / avgsTotal)
    }}
    featurizedRdd.cache()
    println("Number of records in featurized dataset: " + featurizedRdd.count)
    println("Selected feature vectors:")
    featurizedRdd.filter{t => {
      t._1 == "en Computer_science" || t._1 == "en Machine_learning"
    }}.collect.map{t => t._1 -> t._2.mkString("[",",","]")}.foreach(println)
    featurizedRdd.map{t => t._1 -> t._2.mkString(",")}.saveAsSequenceFile("hdfs://"+hostname+":9000/wikistats_featurized")
   }
   ~~~
   </div>


   -  We'll start by entering Spark and loading the data. First, launch a Spark shell.

      ~~~
      cd /root/
      /root/spark/spark-shell
      ~~~

      Next, load the data.

      ~~~
      val data = sc.textFile("/wikistats_20090505-07_restricted")
      ~~~

    - Next, for every line of data, we collect a tuple with elements described next. The first element is what we will call the "full document title", a concatenation of the project code and page title. The second element is a key-value pair whose key is the hour from the `<date-time>` field and whose value is the number of views that occurred in this hour.

      There are a few new points to note about the code below. First, `data.map` takes each line of data in the RDD data and applies all of the code contained in the curly braces after the `=>` symbol. The last line of code is automatically output. The first line of code within the curly braces splits the line of data into the five data fields we discussed in the Spark exercises above. The second line of code within the braces extracts the hour information from the `<date-time>` string. The final line forms the output tuple.

      ~~~
      val featureMap = data.map(line => {
        val Array(dateTime, projectCode, pageTitle, numViews, numBytes) = line.trim.split("\\s+")
        val hour = dateTime.substring(9, 11).toInt
        (projectCode+" "+pageTitle, hour -> numViews.toInt)
      })
      ~~~

      To double-check that your code did what you wanted it to do, you can print the first 10 output tuples:

      ~~~
      featureMap.take(10).foreach(println)
      ~~~

      Now we want to find the average hourly views for each article (average for the same hour across different days).

      In the code below, we first take our tuples in the RDD `featureMap` and, treating the first elements as keys and the second elements as values, group all the values for a single key (i.e. a single article) together using `groupByKey`.  We put the article name in article and the multiple tuples of hours and pageviews in `hoursViews`. The syntax `Array.fill[Int](24)(0)` initializes an integer array of 24 elements with a value of 0 at every element. The for loop then collects the number of days for which we have a particular hour of data in `counts[hour]` and the total pageviews at hour across all these days in `sums[hour]`. Finally, we use the syntax sums zip counts to make an array of tuples with parallel elements from the sums and counts arrays and use this to calculate the average pageviews at particular hours across days in the data set.

      ~~~
      val featureGroup = featureMap.groupByKey.map(grouped => {
        val (article, hoursViews) = grouped
        val sums = Array.fill[Int](24)(0)
        val counts = Array.fill[Int](24)(0)
        for((hour, numViews) <- hoursViews) {
          sums(hour) += numViews
          counts(hour) += 1
        }
        val avgs: Array[Double] =
          for((sum, count) <- sums zip counts) yield
            if(count > 0) sum/count.toDouble else 0.0
        article -> avgs
      })
      ~~~

      Using `println` directly here as above doesn't let us see what’s inside the arrays. The `mkString` method prints an array by concatenating all of its elements with some specified delimiter.  Note that when we use `_1` to access the first part of a tuple, the indexing is 1 and 2 for the first and second parts, not 0 and 1.

      ~~~
      featureGroup.take(10).foreach(x => println(x._1, x._2.mkString(" ")))
      ~~~

    - Now suppose we’re only interested in those articles that were viewed at least once in each hour during the data collection time.

      To do this, we filter to find those articles with an average number of views (the second tuple element in an article tuple) greater than zero in every hour.

      ~~~
      val featureGroupFiltered = featureGroup.filter(t => t._2.forall(_ > 0))
      ~~~

    - So far article popularity is still implicitly in our feature vector (the sum of the average views per hour is the average views per day if the number of days of data is constant across hours).  Since we are interested only in which times are more popular viewing times for each article, we next divide out by this sum.

      If you were following along with the AMP Camp lectures, note that this normalization is different from standardizing each feature separately but accomplishes the goal that all features are on a comparable scale.

      ~~~
      val featurizedRDD = featureGroupFiltered.map(t => {
        val avgsTotal = t._2.sum
        t._1 -> t._2.map(_ /avgsTotal)
      })
      ~~~

      We can use the same command as before to view the latest RDD.

      ~~~
      featurizedRDD.take(10).foreach(x => println(x._1, x._2.mkString(" ")))
      ~~~

    - Save the RDD within Spark and to a file for later use.
      Locally, we just cache the RDD.

      ~~~
      featurizedRDD.cache
      ~~~

      To save to file, we first create a string of comma-separated values for each data point.

      ~~~
      featurizedRDD.map(t => t._1 -> t._2.mkString(",")).saveAsSequenceFile("/wikistats_featurized")
      ~~~

2. In this exercise, we examine the preprocessed data.

    - Count the number of records in the preprocessed data.  Recall that we potentially threw away some data when we filtered out records with zero views in a given hour.

      ~~~
      featurizedRDD.count
      ~~~

      <div class="solution" markdown="1">
      Number of records in the preprocessed data: 802450
      </div>


   - Print the feature vectors for the Wikipedia articles with project code “en” and the following titles: Computer_science, Machine_learning.  The second line below shows another option for printing arrays in a readable way at the command line.

     ~~~
     val featuresCSML = featurizedRDD.filter(t => t._1 == "en Computer_science" || t._1 == "en Machine_learning").collect
     featuresCSML.foreach(x => println(x._1 + "," + x._2.mkString(" ")))
     ~~~

     <div class="solution">
     <textarea rows="12" style="width: 100%" readonly>
     (en Machine_learning, [0.03708182184602984,0.027811366384522376,0.031035872632003234,0.033454252317613876,0.033051189036678766,0.023780733575171308,0.03224506247480856,0.029826682789197912,0.04997984683595326,0.04433696090286176,0.04997984683595326,0.04474002418379687,0.04272470777912134,0.054816606207174545,0.054816606207174545,0.04474002418379687,0.054010479645304324,0.049173720274083045,0.049173720274083045,0.05038291011688836,0.04594921402660219,0.04957678355501815,0.03667875856509473,0.030632809351068126])
     (en Computer_science, [0.03265137425087828,0.057656540607563554,0.03306468278569953,0.033374664186815464,0.03709444100020666,0.03947096507542881,0.03502789832610044,0.03637115106426948,0.036577805331680105,0.0421574705517669,0.04267410622029345,0.03885100227319695,0.03885100227319695,0.046083901632568716,0.04691051870221121,0.050320314114486474,0.05259351105600331,0.04649721016738996,0.04732382723703245,0.048357098574085565,0.04236412481917752,0.043190741888820015,0.03626782393056417,0.03626782393056417])
     </textarea>
     </div>

## Clustering

Now, try to solve the following problem using Spark. We provide less guidance for this problem. If you run out of time, or get stuck or are just curious, again feel free to jump straight to our solutions.

1. We now further explore the featurized dataset via K-means clustering. Implement K-means clustering (as a standalone Spark program) and generate 10 clusters from the featurized dataset created above. For each cluster, print its centroid vector and the " " strings of 10 articles assigned to that cluster.


   <div class="solution" markdown="1">

   Place the following code in a file and use `sbt/sbt run` to run it:

   ~~~
   import spark.SparkContext
   import spark.SparkContext._
   import spark.util.Vector
   
   import scala.util.Random
   import scala.io.Source
   
   object WikipediaKMeans {
     def parseVector(line: String): Vector = {
         return new Vector(line.split(',').map(_.toDouble))
     }
     
     def closestPoint(p: Vector, centers: Array[Vector]): Int = {
       var index = 0
       var bestIndex = 0
       var closest = Double.PositiveInfinity
     
       for (i <- 0 until centers.length) {
         val tempDist = p.squaredDist(centers(i))
         if (tempDist < closest) {
           closest = tempDist
           bestIndex = i
         }
       }
     
       return bestIndex
     }
   
     def main(args: Array[String]) {
       val sparkHome = "/root/spark"
       val jarFile = "target/scala-2.9.2/wikipedia-kmeans_2.9.2-0.0.jar"
       val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
       val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
   
       val sc = new SparkContext(master, "WikipediaKMeans", sparkHome, Seq(jarFile))
   
       val K = 4
       val convergeDist = 1e-6
       var iter = 0
   
       val data = sc.sequenceFile[String, String](
           "hdfs://" + masterHostname + ":9000/wikistats_featurized").map(
               t => (t._1,  parseVector(t._2))).cache()
   
       var centroids = data.sample(false, 0.005, 23789).map(x => x._2).collect().take(K)
       println("Done selecting initial centroids")
   
       var tempDist = 1.0
       while(tempDist > convergeDist) {
         var closest = data.map(p => (closestPoint(p._2, centroids), (p._2, 1)))
         
         var pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
         
         var newCentroids = pointStats.map {pair => (pair._1, pair._2._1 / pair._2._2)}.collectAsMap()
         
         tempDist = 0.0
         for (i <- 0 until K) {
           tempDist += centroids(i).squaredDist(newCentroids(i))
         }
         
         for (newP <- newCentroids) {
           centroids(newP._1) = newP._2
         }
         iter += 1
         println("Finished iteration " + iter + " (delta = " + tempDist + ")")
       }
   
       println("Centroids with some articles:")
       val numArticles = 10
       for((centroid, centroidI) <- centroids.zipWithIndex) {
         // print centroid
         println(centroid.elements.mkString("[",",","]"))
   
         // print numArticles articles which are assigned to this centroid’s cluster
         data.filter(p => (closestPoint(p._2, centroids) == centroidI)).take(numArticles).foreach(
             x => println(x._1))
         println()
       }
   
       sc.stop()
       System.exit(0)
     }
   }
   ~~~

   </div>

2. Run K-means again (so that the cluster centroids are initialized differently) and see how the clusters change.

   <div class="solution" markdown="1">

   Centroids with some articles for K-means with K=10:

   <textarea rows="12" style="width: 100%" readonly>
   {% include 10_clusters_solution.txt %}
   </textarea>

   </div>

3. Repeat #2, now using 50 clusters.

   <div class="solution" markdown="1">

   Centroids with some articles for K-means with K=50:

   <textarea rows="12" style="width: 100%" readonly>
   {% include 50_clusters_solution.txt %}
   </textarea>

   </div>


