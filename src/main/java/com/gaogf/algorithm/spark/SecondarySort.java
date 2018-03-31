package com.gaogf.algorithm.spark;

import com.gaogf.algorithm.util.SparkTupleComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by gaogf on 2018/3/31.
 */
public class SecondarySort {
    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Usage: SecondarySort <file>");
            System.exit(1);
        }
        String input = args[0];
        JavaSparkContext sparkContext = new JavaSparkContext();
        JavaRDD<String> rdd = sparkContext.textFile(input, 1);
        JavaPairRDD<String, Tuple2<Integer, Integer>> pair = rdd.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                String[] tokens = s.split(",");
                Integer time = new Integer(tokens[1]);
                Integer value = new Integer(tokens[2]);
                Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>(time, value);
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
            }
        });
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pair.collect();
        for (Tuple2 t: output) {
            Tuple2<Integer,Integer> timevalue = (Tuple2<Integer,Integer>)t._2();
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._1);
        }
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groupByKey = pair.groupByKey();
        System.out.println("=======DEBUG=======");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groupByKey.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t: output2) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer,Integer> t2: list) {
                System.out.println(t2._1 + "," + t2._2);
            }
        }
        System.out.println("======================");
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groupByKey.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> s) throws Exception {
                List<Tuple2<Integer, Integer>> arrayList = new ArrayList<Tuple2<Integer, Integer>>(iterableToList(s));
                Collections.sort(arrayList, new SparkTupleComparator());
                return arrayList;
            }
        });
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer,Integer>>> t: output3){
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            for (Tuple2<Integer,Integer> t2: list){
                System.out.println(t2._1 + "," + t2._2);
            }
        }
    }
    public static List<Tuple2<Integer,Integer>> iterableToList(Iterable<Tuple2<Integer,Integer>> iterable){
        List<Tuple2<Integer,Integer>> tuple2List = new ArrayList<Tuple2<Integer, Integer>>();
        for (Tuple2<Integer,Integer> t: iterable) {
            tuple2List.add(t);
        }
        return tuple2List;
    }
}
