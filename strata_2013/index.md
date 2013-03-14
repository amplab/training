---
layout: global
title: Introduction
next: overview-of-the-exercises.html
---

Welcome to the AMP Camp hands-on big data mini course. These training materials have been produced by members of the open source community, especially PhD students in the UC Berkeley AMPLab with the support from professors and students from ECNU.

This mini course consists of a series of exercises that will have you working directly with components of Hadoop as well as components of our open-source software stack, called the <a href="https://amplab.cs.berkeley.edu/bdas/">Berkeley Data Analytics Stack (BDAS)</a>, that have been released and are ready to use.
These components include [Spark](http://spark-project.org), Spark Streaming, and [Shark](http://shark.cs.berkeley.edu).

We will begin by walking you through the process of connecting to our server, which has all the necessary software installed. Then we will dive into big data analytics training exercises, starting with simple interactive analysis techniques at the Spark and Shark shells, followed by implementing some more advanced machine learning algorithms to incorporate into your analysis.

# Course Prerequisites
This mini course is meant to be hands-on introduction to Spark and Shark. While Shark supports a simplified version of SQL, Spark supports multiple languages (Java, Scala, Python). For the sections about Spark, the mini course allows you to choose which language you want to use as you follow along and gain experience with the tools. The following table shows which languages this mini course supports for each section. You are welcome to mix and match languages depending on your preferences and interests.

<center>
<style type="text/css">
table td, table th {
  padding: 5px;
}
</style>
<table class="bordered">
<thead>
<tr>
  <th>Section</th>
    <th><img src="img/scala-sm.png"/></th>
    <th><img src="img/java-sm.png"/></th>
    <th><img src="img/python-sm.png"/>
  </th>
</tr>
</thead><tbody>
<tr>
  <td>Spark Interactive</td>
  <td class="yes">yes</td>
  <td class="no">no</td>
  <td class="yes">yes</td>
</tr><tr>
  <td>Shark (SQL)</td>
  <td colspan="3" class="yes">All SQL</td>
</tr><tr>
  <td class="dimmed"><b>Optional:</b> Machine Learning :: featurization</td>
  <td class="dimmed yes">yes</td>
  <td class="dimmed no">no</td>
  <td class="dimmed yes">yes</td>
</tr><tr>
  <td>Machine Learning :: K-Means</td>
  <td class="yes">yes</td>
  <td class="yes">yes</td>
  <td class="yes">yes</td>
</tr>
</tbody>
</table>
</center>

# Logging onto the cluster

We have already setup Spark and Shark on a cluster and created 20 accounts. You should be able to ssh to the cluster.


