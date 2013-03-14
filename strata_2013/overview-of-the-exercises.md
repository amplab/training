---
layout: global
title: Overview Of The Exercises
prev: index.html
next: introduction-to-the-scala-shell.html
---

The exercises in this mini course are divided into sections designed to give a hands-on experience with Spark, Shark and Spark Streaming.
For Spark, we will walk you through using the Spark shell for interactive exploration of data. You have the choice of doing the exercises using Scala or using Python.
For Shark, you will be using SQL in the Shark console to interactively explore the same data.
For Spark Streaming, we will walk you through writing stand alone Spark programs in Scala to processing Twitter's sample stream of tweets.
Finally, you will have to complete a complex machine learning exercise which will test your understanding of Spark.

## Dataset For Exploration
We have loaded 3GB of Wikipedia traffic statistics data obtained from http://aws.amazon.com/datasets/4182 .
To make the analysis feasible (within the short timeframe of the exercise), we took a sample of the data.
You can list the files:

<pre class="prettyprint lang-bsh">
ls -l /dev/ampcamp/data/wikistats
</pre>

There are 11 files.

The data are partitioned by date and time.
Each file contains traffic statistics for all pages in a specific hour.
Let's take a look at the file:

<pre class="prettyprint lang-bsh">
less /dev/ampcamp/data/wikistats/part-00100
</pre>

The first few lines of the file are copied here:

<pre class="prettyprint lang-bsh">
20090505-040000 aa.b Main_Page 1 14266
20090505-040000 aa.b MediaWiki:Yourname 1 5434
20090505-040000 aa.b User_talk:WOPR 1 5731
20090505-040000 aa.b Wikibooks:About 1 15719
20090505-040000 aa.b Wikibooks:General_disclaimer 1 16108
20090505-040000 aa.d de:Benutzer:Purodha 1 1181
20090505-040000 aa File:Commons-logo.svg 1 29221
20090505-040000 aa File_talk:Commons-logo.svg 2 25647
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

