import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.*;
import java.util.*;
import com.google.common.collect.Lists;

import scala.Tuple2;
import spark.api.java.*;
import spark.api.java.function.*;
import spark.util.Vector;


public class WikipediaKMeans {
  // Add any new functions you need here

  public static void main(String[] args) throws Exception {
    Logger.getLogger("spark").setLevel(Level.WARN);
    JavaSparkContext sc = new JavaSparkContext("local[6]", "WikipediaKMeans");

    int K = 10;
    double convergeDist = .000001;

    JavaPairRDD<String, Vector> data = sc.textFile(
      "/dev/ampcamp/imdb_data/wikistats_featurized").map(
      new PairFunction<String, String, Vector>() {
        public Tuple2<String, Vector> call(String in)
        throws Exception {
          String[] parts = in.split("#");
          return new Tuple2<String, Vector>(
            parts[0], JavaHelpers.parseVector(parts[1]));
        }
      }
     ).cache();

    // Your code goes here
    sc.stop();
    System.exit(0);
  }
}
