/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment4;


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.*;
import java.util.Map.Entry;

public class Classification {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
    DataSource<String> sumInput = env.readTextFile(Config.pathToSums());

    DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());
    DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

    DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

    DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData.map(new Classifier())
        .withBroadcastSet(conditionals, "conditionals")
        .withBroadcastSet(sums, "sums");

    classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

    @Override
    public Tuple3<String, String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple3<String, String, Long>(elements[0], elements[1], Long.parseLong(elements[2]));
    }
  }

  public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple2<String, Long>(elements[0], Long.parseLong(elements[1]));
    }
  }


  public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>>  {

     private final Map<String, Map<String, Long>> wordCounts = Maps.newHashMap();
     private final Map<String, Long> wordSums = Maps.newHashMap();
     private final Long smoothingParameter = 3L;


      @Override
     public void open(Configuration parameters) throws Exception {
       super.open(parameters);

         List<Tuple2<String, Long>> sums = getRuntimeContext().getBroadcastVariable("sums");

         for (Tuple2<String, Long> t2 : sums) {
             wordSums.put(t2.f0, t2.f1);
         }

         List<Tuple3<String, String, Long>> conditionals = getRuntimeContext().getBroadcastVariable("conditionals");

         for (Tuple3<String, String, Long> t3 : conditionals ) {

             if (!wordCounts.containsKey(t3.f0)) {
                 wordCounts.put(t3.f0, new HashMap<String, Long>());
             }
             wordCounts.get(t3.f0).put(t3.f1, t3.f2);
         }

     }

     @Override
     public Tuple3<String, String, Double> map(String line) throws Exception {

         String[] tokens = line.split("\t");
         String label = tokens[0];
         String[] words = tokens[1].split(",");

         Set<String> wordsSet = new HashSet<String>(Arrays.asList(words));
         double maxProbability = Double.NEGATIVE_INFINITY;

         String predictionLabel = "";

         Iterator entries = wordSums.entrySet().iterator();
         while (entries.hasNext()) {

             Entry entry = (Entry) entries.next();
             String lab = (String) entry.getKey();
             Long wordSumForLabel = (Long) entry.getValue();

             Double prob = Math.log( wordSumForLabel );

             Map<String, Long> currentTermsForLabel = wordCounts.get(lab);


             for (String word : words) {

                 Long termCountForLabel = currentTermsForLabel.get(word);

                 if (termCountForLabel != null) {
                     prob += Math.log( termCountForLabel +  smoothingParameter);
                 } else {
                     prob += Math.log( smoothingParameter );
                 }
             }

             double den = wordSums.get(lab) + (smoothingParameter * wordsSet.size());

             prob -= ( words.length * Math.log( den ) );

             if ( prob > maxProbability) {
                 predictionLabel = lab;

                 maxProbability = prob;
             }

         }

       return new Tuple3<String, String, Double>(label, predictionLabel, maxProbability);


     }


   }

}
