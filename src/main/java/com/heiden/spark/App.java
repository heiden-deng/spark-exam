package com.heiden.spark;

import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//import java.util.Arrays;
//import java.util.List;
//import scala.collection.immutable.List;




/**
 * Hello world!
 *
 */
public class App 
{

    public static void flatMap(JavaSparkContext sc){
        List<String> list = Arrays.asList("张无忌 赵敏","宋青书 猪头");
        JavaRDD<String> listRDD = sc.parallelize(list);

        JavaRDD<String> nameRDD = listRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(name -> "hello " + name);
        nameRDD.foreach(name -> System.out.println(name));

    }

    public static void mapPartitions(JavaSparkContext sc){
        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> listRDD = sc.parallelize(list,2);
        listRDD.mapPartitions(iterator -> {
            ArrayList<String> array = new ArrayList<>();
            while(iterator.hasNext()){
                array.add("hello " + iterator.next());
            }
            return array.iterator();
        }).foreach(name -> System.out.println(name));
    }

    public static void reduce(JavaSparkContext sc){
        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        Integer result = listRDD.reduce((a,b) -> a+b);
        System.out.println("sum result = " + result);
    }


    public static void reduceByKey(JavaSparkContext sc){
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String,Integer>("武当",99),
                new Tuple2<String,Integer>("少林",97),
                new Tuple2<>("武当",89),
                new Tuple2<>("少林",77)
                );
        JavaPairRDD<String,Integer> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String,Integer> resultRDD = listRDD.reduceByKey((x,y) -> x+y);
        resultRDD.foreach(tuple -> System.out.println("门派:" + tuple._1 + "->" + tuple._2));

    }

    public static void groupByKey(JavaSparkContext sc){
        List<Tuple2<String, String>> list = Arrays.asList(new Tuple2<String,String>("武当","张三丰"),
                new Tuple2<String,String>("少林","和尚1"),
                new Tuple2<>("武当","宋青书"),
                new Tuple2<>("少林","和尚2"),
                new Tuple2<String,String>("峨眉","灭绝师太")
        );
        JavaPairRDD<String,String> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String,Iterable<String>> groupByKeyRDD = listRDD.groupByKey();
        groupByKeyRDD.foreach(tuple -> {
            String menpai = tuple._1;
            Iterator<String> renwu = tuple._2.iterator();
            String people = "";
            while(renwu.hasNext()){
                people += renwu.next() + " ";
            }
            System.out.println("门派:" + menpai + " 人员:" + people);
        });
    }


    public static void join(JavaSparkContext sc){
        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );

        final JavaPairRDD<Integer,String> namerdd = sc.parallelizePairs(names);
        final JavaPairRDD<Integer,Integer> scoresrdd = sc.parallelizePairs(scores);

        final JavaPairRDD<Integer,Tuple2<String,Integer>> namescorerdd = namerdd.join(scoresrdd);
        namescorerdd.foreach(tuple -> System.out.println("学号:" + tuple._1 + ",姓名:" + tuple._2._1 + ",成绩:" + tuple._2._2));

    }

    public static void cartesion(JavaSparkContext sc){
        List<String> list1 = Arrays.asList("A","B");
        List<Integer> list2 = Arrays.asList(1,2,3);
        JavaRDD<String> list1rdd = sc.parallelize(list1);
        JavaRDD<Integer> list2rdd = sc.parallelize(list2);
        list1rdd.cartesian(list2rdd).foreach(tuple -> System.out.println(tuple._1 +"->" + tuple._2));
    }


    public static void filter(JavaSparkContext sc){
        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filterRDD = listRDD.filter(num -> num % 2 == 0);
        filterRDD.foreach(num -> System.out.println(num + " "));
    }


    public static void intersection(JavaSparkContext sc){
        List<Integer> list1 = Arrays.asList(1,2,3,4);
        List<Integer> list2 = Arrays.asList(3,4,5,6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.intersection(list2RDD).foreach( num -> System.out.println(num));
    }
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("flatmap").setMaster(args[0]);//setMaster("spark://localhost:7077");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //flatMap(sc);

        System.out.println( "========================" );
        mapPartitions(sc);

        System.out.println( "========================" );
        reduce(sc);

        System.out.println( "========================" );
        reduceByKey(sc);

        System.out.println( "========================" );
        groupByKey(sc);


        System.out.println( "========================" );
        join(sc);

        System.out.println( "========================" );
        cartesion(sc);

        System.out.println( "========================" );
        filter(sc);

        System.out.println( "==============intersect==========" );
        intersection(sc);
        System.out.println( "Hello World!" );
    }
}
