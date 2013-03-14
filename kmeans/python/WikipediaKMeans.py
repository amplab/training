"""
KMeans clustering of Wikipedia pages using Spark.
"""
import os
import sys
import numpy as np

from pyspark import SparkContext

def setClassPath():
    oldClassPath = os.environ.get('SPARK_CLASSPATH', '')
    cwd = os.path.dirname(os.path.realpath(__file__))
    os.environ['SPARK_CLASSPATH'] = cwd + ":" + oldClassPath

def parseVector(line):
    return np.array([float(x) for x in line.split(',')])

# Add any new functions you need here

if __name__ == "__main__":
    setClassPath()
    sc = SparkContext("local[6]", "PythonKMeans")
    K = 10
    convergeDist = 1e-5

    lines = sc.textFile("/dev/ampcamp/imdb_data/wikistats_featurized")
    data = lines.map(
	lambda x: (x.split("#")[0], parseVector(x.split("#")[1]))).cache()

    # Your code goes here
