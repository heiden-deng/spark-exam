package com.heiden.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class PeopleInfoCalculator {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("PeopleinfoCalc").setMaster(args[0]);//setMaster("spark://localhost:7077");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> dataFile = sc.textFile(args[1]);

        JavaRDD<String> maleFilterData = dataFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("M");
            }
        });
        JavaRDD<String> femaleFilterData = dataFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("F");
            }
        });

        JavaRDD<String> maleHeightData = maleFilterData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[2]).iterator();
            }

        });
        JavaRDD<String> femaleHeightData = femaleFilterData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")[2]).iterator();
            }
        });
        JavaRDD<Integer> maleHeightDataInt = maleHeightData.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return Integer.parseInt(String.valueOf(s));
            }
        });

        JavaRDD<Integer> femaleHeightDataInt = femaleHeightData.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return Integer.parseInt(String.valueOf(s));
            }
        });

        JavaRDD<Integer> maleHeightLowSort = maleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        },true,3);

        JavaRDD<Integer> femaleHeightLowSort = femaleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        },true,3);

        JavaRDD<Integer> maleHeightHighSort = maleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        },false,3);

        JavaRDD<Integer> femaleHeightHignSort = femaleHeightDataInt.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        },false,3);

        Integer lowestMale = maleHeightLowSort.first();
        Integer lowestFemale = femaleHeightLowSort.first();
        Integer highestMale = maleHeightHighSort.first();
        Integer highestFemale = femaleHeightHignSort.first();
        System.out.println("Number of Female People:" + femaleHeightData.count());
        System.out.println("Number of Male People:" + maleHeightData.count());
        System.out.println("Lowest Female:" + lowestFemale);
        System.out.println("Lowest Male:" + lowestMale);
        System.out.println("Highest Male:" + highestMale);
        System.out.println("Highest Female:" + highestFemale);
    }
}
