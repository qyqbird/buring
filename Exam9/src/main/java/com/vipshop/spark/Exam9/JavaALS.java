/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vipshop.spark.Exam9;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * Example using MLLib ALS from Java.
 */
public final class JavaALS {
  static class ParseRating extends Function<String, Rating> {
    private static final Pattern COMMA = Pattern.compile(",");

    @Override
    public Rating call(String line) {
      String[] tok = COMMA.split(line);
      int x = Integer.parseInt(tok[0]);
      int y = Integer.parseInt(tok[1]);
      double rating = Double.parseDouble(tok[2]);
      return new Rating(x, y, rating);
    }
  }

  static class FeaturesToString extends Function<Tuple2<Object, double[]>, String> {
    @Override
    public String call(Tuple2<Object, double[]> element) {
      return element._1() + "," + Arrays.toString(element._2());
    }
  }

  static class ParseUserProduct extends Function<String,Tuple2<Object,Object>>{
    @Override
    public Tuple2<Object, Object> call(String arg0) throws Exception {
        String[] sp = arg0.split(",");
        return new Tuple2<Object,Object>(Integer.parseInt(sp[0]),Integer.parseInt(sp[1]));
    }
  }
  public static void main(String[] args) {

    if (args.length != 5 && args.length != 6) {
      System.err.println(
          "Usage: JavaALS <master> <ratings_file> <rank> <iterations> <output_dir> [<blocks>]");
      System.exit(1);
    }
    
    
    int rank = Integer.parseInt(args[2]);
    int iterations = Integer.parseInt(args[3]);
    String outputDir = args[4];
    int blocks = -1;
    if (args.length == 6) {
      blocks = Integer.parseInt(args[5]);
    }
    System.out.println("------------1---------------------");
    System.setProperty("spark.executor.memory", "2g");
    JavaSparkContext sc = new JavaSparkContext(args[0], "JavaALS",
        System.getenv("SPARK_HOME"), "target/Exam9-0.0.1-SNAPSHOT.jar");
    System.out.println("------------2---------------------");
    JavaRDD<String> lines = sc.textFile(args[1]);
    System.out.println("------------3---------------------");
    JavaRDD<Rating> ratings = lines.map(new ParseRating());
    System.out.println("------------4---------------------");
    //JavaRDD<Tuple2<Object,Object>> usersProducts = lines.map(new ParseUserProduct());
    
    
    MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);
    JavaRDD<String> tt = model.userFeatures().toJavaRDD().map(new FeaturesToString());
    System.out.println("------------5---------------------");
    
    System.out.println("-------------------------10------------------");
    tt.saveAsTextFile(outputDir + "/userFeatures");
    System.out.println("------------6---------------------");
    //model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
     //   outputDir + "/userFeatures");
    System.out.println("------------7---------------------");
    model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
        outputDir + "/productFeatures");
    //model.predict(usersProducts.rdd()).saveAsTextFile(outputDir);
    System.out.println("Final user/product features written to " + outputDir);
    
    System.exit(0);
  }
}
