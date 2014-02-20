package com.vipshop.spark.Exam9;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.setProperty("user.name", "hdfs");
        System.setProperty("HADOOP_USER_NAME","hdfs");
        System.out.println( "Hello World!" );
        System.setProperty("spark.executor.memory", "1g");
        
        Map<String, String> envs = new HashMap<String, String>();
        envs.put("HADOOP_USER_NAME", "hdfs");
        System.out.println("--------------1-----------------");
        JavaSparkContext sc = new JavaSparkContext(args[0], "App",
                System.getenv("SPARK_HOME"), "target/Exam9-0.0.1-SNAPSHOT.jar");
        System.out.println("--------------2-----------------");
        JavaRDD<String> jr = sc.textFile(args[1]);
        System.out.println("--------------3-----------------");
        jr.saveAsTextFile(args[2]);
        System.out.println("**************************");
    }
    
}
