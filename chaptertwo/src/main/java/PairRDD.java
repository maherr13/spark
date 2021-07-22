import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRDD {
    public static void main(String[] args) {
        List<String> ls = new ArrayList<>();
        ls.add("Error:e1");
        ls.add("Fatal:f1");
        ls.add("Warn:w1");
        ls.add("Error:e1");
        ls.add("Fatal:f1");
        ls.add("Warn:w1");
        ls.add("Error:e1");
        ls.add("Fatal:f1");
        ls.add("Warn:w1");
        ls.add("Fatal:f1");
        ls.add("Warn:w1");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("pair").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //This program counts How many msg type are there in a file
        //Using Pair RDD
        sc.parallelize(ls)
          .mapToPair(line -> new Tuple2<String, Long>(line.split(":")[0], 1L))
          .reduceByKey((v1, v2)-> v1+v2)
          .foreach(t -> System.out.println(t._1 + " has "+ t._2 + " instances"));

        sc.close();
    }
}
