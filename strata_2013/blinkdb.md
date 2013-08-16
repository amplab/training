# Data Exploration Using BlinkDB

BlinkDB is a large-scale data warehouse system like Shark that adds the ability to create and use smaller samples of large datasets to make queries even faster.  Today you're going to get a sneak peek at an Alpha release of BlinkDB.  We'll set up BlinkDB and use it to run some SQL queries against the English Wikipedia.  If you've already done the Shark exercises, you might notice that we're going to go through the same exercises.  Don't worry if you haven't used Shark, though - we haven't assumed that you have.

1. First, launch the BlinkDB console:

   <pre class="prettyprint lang-bsh">
   /root/blinkdb/bin/blinkdb-withinfo
   </pre>

2. Similar to Apache Hive, BlinkDB can query external tables (i.e. tables that are not created in BlinkDB).
   Before you do any querying, you will need to tell BlinkDB where the data is and define its schema.

   <pre class="prettyprint lang-sql">
   blinkdb> create external table wikistats (dt string, project_code string, page_name string, page_views int, bytes int) row format delimited fields terminated by ' ' location '/wiki/pagecounts';
   <span class="nocode">
   ...
   Time taken: 0.232 seconds
   13/02/05 21:31:25 INFO CliDriver: Time taken: 0.232 seconds</span></pre>

   <b>FAQ:</b> If you see the following errors, don’t worry. Things are still working under the hood.

   <pre class="nocode">
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.core.resources" but it cannot be resolved.
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.core.runtime" but it cannot be resolved.
   12/08/18 21:07:34 ERROR DataNucleus.Plugin: Bundle "org.eclipse.jdt.core" requires "org.eclipse.text" but it cannot be resolved.</pre>

   <b>FAQ:</b> If you see errors like these, you might have copied and pasted a line break, and should be able to remove it to get rid of the errors.

   <pre>13/02/05 21:22:16 INFO parse.ParseDriver: Parsing command: CR
   FAILED: Parse Error: line 1:0 cannot recognize input near 'CR' '&lt;EOF&gt;' '&lt;EOF&gt;'</pre>

3. Let's create a table containing all English records and cache it in the cluster's memory.

   <pre class="prettyprint lang-sql">
   blinkdb> create table wikistats_cached as select * from wikistats where project_code="en";
   <span class="nocode">
   ...
   Time taken: 127.5 seconds
   13/02/05 21:57:34 INFO CliDriver: Time taken: 127.5 seconds</span></pre>

4. Now let's create a 1% random sample of this table using the samplewith operator and cache it in the cluster's memory.

   <pre class="prettyprint lang-sql">
   blinkdb> create table wikistats_sample_cached as select * from wikistats_cached samplewith 0.01;
   <span class="nocode">
   ...
   Moving data to: hdfs://localhost:54310/user/hive/warehouse/wikipedia_sample
   Time taken: 4.185 seconds
   12/08/18 21:19:07 INFO CliDriver: Time taken: 4.185 seconds</span></pre>

5. Next Compute a simple count of the number of English records of the original table.  For now, we're not using the sample at all.  (If you have some familiarity with databases, note that we use the "`count(1)`" syntax here since in earlier versions of Hive, the more popular "`count(*)`" operation was not supported. BlinkDB supports most of Hive SQL; its syntax is described in detail in the <a href="https://cwiki.apache.org/confluence/display/Hive/GettingStarted" target="_blank">Hive Getting Started Guide</a>.)Now try the same thing on the original table, without sampling.

   <pre class="prettyprint lang-sql">
   blinkdb> select count(1) from wikistats_cached;
   <span class="nocode">
   ...
   122352588
   Time taken: 7.632 seconds
   12/08/18 21:23:13 INFO CliDriver: Time taken: 7.632 seconds</span></pre>

6. Now approximate the same count using the sampled table.  In the Alpha release, you need to tell BlinkDB to compute an approximation by prepending "`approx_`" to your aggregation function.  (Also, only queries that compute `count`, `sum`, `average`, or `stddev` can be approximated.  In the future many more functions, including UDFs, will be approximable.)

   <pre class="prettyprint lang-sql">
   blinkdb> select approx_count(1) from wikistats_sampled;
   <span class="nocode">
   ...
   122352588 1000
   Time taken: 7.632 seconds
   12/08/18 21:23:13 INFO CliDriver: Time taken: 7.632 seconds</span></pre>

   Notice that our sampled query produces a slightly incorrect answer, but it runs faster.  Also, the query result now includes a second column, which tells us how close BlinkDB thinks it is to the true answer.  (If you know a bit of statistics, the interval [first value - second value, first value + second value] is a .99 confidence interval for the true count.  The confidence level can be changed to x by appending "with confidence x" to the query.)

7. Compute the total traffic to Wikipedia English pages for each hour between May 7 and May 9, with one line per hour.

   <pre class="prettyprint lang-sql">
   blinkdb> select dt, approx_sum(page_views) from wikistats_sampled group by dt;
   <span class="nocode">
   ...
   20090507-070000	6292754 1000
   20090505-120000	7304485 1001
   20090506-110000	6609124 999
   Time taken: 12.614 seconds
   13/02/05 22:05:18 INFO CliDriver: Time taken: 12.614 seconds</span></pre>

   Each line in the output includes an error level for the corresponding result.
   
   As before, you can also compute an exact answer by running the same query on the table `wikistats_cached`, replacing "`approx_sum`" with "`sum`".

    <pre class="prettyprint lang-sql">
    blinkdb> select dt, sum(page_views) from wikistats_cached group by dt;
    <span class="nocode">
    ...
    20090507-070000	6292754
    20090505-120000	7304485
    20090506-110000	6609124
    Time taken: 12.614 seconds
    13/02/05 22:05:18 INFO CliDriver: Time taken: 12.614 seconds</span></pre>

8. In the Spark section, we ran a very expensive query to compute pages that were viewed more than 200,000 times. It is fairly simple to do the same thing in SQL.

   To make the query run faster, we increase the number of reducers used in this query to 50 in the first command. Note that the default number of reducers, which we have been using so far in this section, is 1.

   <pre class="prettyprint lang-sql">
   blinkdb> set mapred.reduce.tasks=50;
   blinkdb> select page_name, approx_sum(page_views) as views from wikistats_sampled group by page_name having views > 200000;
   <span class="nocode">
   ...
   index.html      310642
   Swine_influenza 534253
   404_error/      43822489
   YouTube 203378
   X-Men_Origins:_Wolverine        204604
   Dom_DeLuise     396776
   Special:Watchlist       311465
   The_Beatles     317708
   Special:Search  17657352
   Special:Random  5816953
   Special:Export  248624
   Scrubs_(TV_series)      234855
   Cinco_de_Mayo   695817
   2009_swine_flu_outbreak 237677
   Deadpool_(comics)       382510
   Wiki    464935
   Special:Randompage      3521336
   Main_Page       18730347
   Time taken: 68.693 seconds
   13/02/26 08:12:42 INFO CliDriver: Time taken: 68.693 seconds</span></pre>

9. With all the warm up, now it is your turn to write queries. Write Hive QL queries to answer the following questions:

- Count the number of distinct date/times for English pages.  Try the same query on the original table and compare the results.  The kind of sampling available in the Alpha is not very effective for queries that depend on rare values, like `count distinct`.

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-sql">
   select count(distinct dt) from wikistats_sampled;</pre>
   </div>

- How many hits are there on pages with Berkeley in the title throughout the entire period?

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-sql">
   select count(page_views) from wikistats_sampled where page_name like "%berkeley%";
   /* "%" in SQL is a wildcard matching all characters. */</pre>
   </div>

- Generate a histogram for the number of hits for each hour on May 6, 2009; sort the output by date/time. Based on the output, which hour is Wikipedia most popular?

   <div class="solution" markdown="1">
   <pre class="prettyprint lang-sql">
   select dt, sum(page_views) from wikistats_sampled where dt like "20090506%" group by dt order by dt;</pre>
   </div>

To exit BlinkDB, type the following at the BlinkDB command line (and don't forget the semicolon!).

   <pre class="prettyprint lang-sql">
   blinkdb> exit;</pre>
