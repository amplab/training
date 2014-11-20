---
layout: global
title: Getting Started
navigation:
  weight: 20
  show: true
skip-chapter-toc: true
---

# Getting Started With Your USB Stick

#### On-site Participants
You should have received a USB stick containing the training materials.  If you do not have one, please ask a TA.

#### Remote Participants
Please download the training materials from
<a href="http://d12yw77jruda6f.cloudfront.net/ampcamp5-usb.zip">here</a>.

## Quick Start

After loading the USB key, you should perform the steps outlined below.  If you
are a windows user, please consider using Powershell, or otherwise replace `cd`
with `dir` and any forward slash (`/`) with a backwards slash (`\`) when
navigating directories.

1. (Recommended, but not required) Copy the contents of the USB drive to somewhere on your hard drive.
It is possible to run the exercises directly from the USB drive, although the exercises will run
much more slowly.

    <p class="alert alert-warn">
    <i class="icon-info-sign">    </i>
    The USB drive contains over 2GB of files, so please begin this file transfer as soon as possible.
    </p>

2. Rename the folder containing all exercise content (either on your hard-drive
   or on the USB key) to `AMPCAMP` (or any other pathname without spaces).  If you
   skip this step, you will see your sbt builds fail.

3. Change directories in to the folder containing all exercise content (either on your hard-drive or on the USB key)

    <p class="alert alert-warn">
    <i class="icon-info-sign">    </i>
 Throughout the training exercises, all command-line instructions of the form
 'usb/$' refer to a terminal that is the root directory containing all exercise
 content (either on your hard-drive or on the USB key) . Moreover, [usb root
 directory] refers to the full path to this directory.
    </p>


4. Check your setup by building a toy application. To do this, run the following commands,
and confirm the results match. If they do, you just built a simple spark application - go look at the
source code to get a feel for what happened.

   ~~~
usb/$ cd simple-app
$ ../sbt/sbt package
$ ../spark/bin/spark-submit --class "SimpleApp" --master local[*] target/scala-2.10/simple-project_2.10-1.0.jar
2014-06-27 15:40:44.788 java[15324:1607] Unable to load realm info from SCDynamicStore
Lines with a: 73, Lines with b: 35
   ~~~

Please ask a TA if you are having trouble getting the simple application to build.

## Additional Required Download

Some of the exercises require you to download addtional project templates and/or data. The
following instructions explain how to incorporate this material with the content of the USB drive.

1. Download the following zip file:
    - Link for on-site participants: <a href="http://d12yw77jruda6f.cloudfront.net/training-downloads.zip">training-downloads.zip</a>
    - Link for remote participants: <a href="http://d12yw77jruda6f.cloudfront.net/training-downloads.zip">training-downloads.zip</a>

2. Unzip the `training-downloads.zip` file to your [usb root directory].

## USB Contents

You'll find the following contents in the USB drive (this info is taken from the README):

 * **spark** - spark binary distribution
     * conf/log4j.properties - WARN used for default level, Snappy warnings silenced
 * **tachyon** - tachyon 0.5.0
 * **data** - example and lab data
     * graphx - graphx lab data
     * movielens - MLlib lab data
     * wiki_parquet - SparkSQL lab data
     * examples-data - examples directory from Spark src
     * join - example files for joins
 * **sbt** - a fresh copy of sbt (v. 0.13.5)
     * sbt-launcher-lib.bash - modified to understand non-default location of bin scripts
     * sbt.bat - modified to understand non-default location of bin scripts [Windows]
     * conf/sbtopts - modified to point to embeded ivy cache
     * conf/sbtconfig.txt - modified to point to embeded ivy cache for [Windows]
     * ivy/cache - a pre-populated cache, pointed to via conf/sbtopts
     * bin - removed and all files moved into sbt's home directory so users can run sbt/sbt similair to working with spark's source code
 * **simple-app** - a simple example app to build (based on the Spark quick start docs)
 * **streaming** - project template for Spark Streaming examples
 * **machine-learning** - project template for Machine Learning examples
 * **website** - documentation for the examples
