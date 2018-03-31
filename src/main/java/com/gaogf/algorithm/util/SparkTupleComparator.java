package com.gaogf.algorithm.util;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by pactera on 2018/3/31.
 */
public class SparkTupleComparator implements Comparator<Tuple2<Integer,Integer>> , Serializable {
    public SparkTupleComparator() {
    }

    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o1._1.compareTo(o2._1);
    }
}
