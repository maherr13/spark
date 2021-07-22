import net.minidev.json.JSONUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.log4j.LogManager.getLogger;

public class SparkTraining {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        List<Integer> ls = new ArrayList<>();
        ls.add(1);
        ls.add(3);
        ls.add(5);
        ls.add(7);
        ls.add(9);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> jr = sc.parallelize(ls);
//        Sum the elements of RDD using reduce function
        Integer result = jr.reduce(( f1,  f2) -> f1 + f2);
        //Storing the sqrt of RDD in anther RDD
        JavaRDD<Double> result2 = jr.map(f -> Math.sqrt (f));

        //print each element in RDD
        result2.foreach(v -> System.out.println(v));
        //if you tried method ref
        //result2.foreach(System.out::println);
        //you will get serialaized  exception case spark will try to serialize the method throw multi CPUs and thats cant be
        //to fix that to transform RDD to java LIST then use the method ref
        //result2.collect().forEach(System.out::println);
        System.out.println(result);
        // Tuples are data type at scala which you can use in Java
        Tuple2<Integer, Integer> t = new Tuple2<>(2,3);
        // you can put the Tuple2 at RDD Type
        JavaRDD<Tuple2<Integer, Integer>> myRDD = jr.map(f -> new Tuple2<>(f, f*2));
        myRDD.foreach(v -> System.out.println("Tuple2 output: "+v));

        sc.close();


    }
}
