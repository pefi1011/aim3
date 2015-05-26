/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
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

package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class AverageFriendFoeRatio {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /* Convert the input to edges, consisting of (source, target, isFriend ) */
    DataSet<Tuple3<Long, Long, Boolean>> edges = input.flatMap(new EdgeReader());

    /* Create a dataset of all vertex ids and count them */
    DataSet<Long> numVertices =
        edges.project(0).types(Long.class) // hole mir die die Src Spalte Werte
            .union(edges.project(1).types(Long.class)) // mache Union mit Target Spalte, wo es keine Ueberlapung gibt (distinct)
            .distinct().reduceGroup(new CountVertices()); // in reduceGroup zaehle wie viele es Vertexes gibt

    /* Zahle wie viele ein vertex Beziehung hat und wie viel davon Freunde und Feinde sind
    * dabei Beruekcsichtige nur Vertices wo mind. ein Freund und Fein vorhanden ist
    * */
    DataSet<Tuple5<Long, Long, Long, Long, Double>> verticesWithFriendsAndFoesDegreeFilteredWithRatio =
        edges.project(0, 2).types(Long.class, Boolean.class)
             .groupBy(0).reduceGroup(new DegreeOfFilteredFriendsAndFoesWithRatio());



      DataSet<Long> numVerticesFiltered = verticesWithFriendsAndFoesDegreeFilteredWithRatio.project(0).types(Long.class).reduceGroup(new CountVertices());

      // I HATE YOUR FLINK DOCUMENTATION AND YOUR ERROR MGS!!!! I LOST 2 HOURS FOR STUPID THING LIKE FIGURING OUT WHAT DATATYPE YOU WANT!
     // Tuple1<Double> avgRatio = verticesWithFriendsAndFoesDegreeFilteredWithRatio.project(4).types(Double.class).aggregate(Aggregations.SUM,4).reduceGroup(new CalculateAvgRatio()).withBroadcastSet(numVertices, "numVertices");

     // I HATE YOUR EXCEPTION MSGS AGAIN! WITHOUT GROUP BY (0) I GET /TMP/FLINK FILE NOT FOUND EXCEPTION ????
     DataSet< Tuple1<Double> > avgRatio = verticesWithFriendsAndFoesDegreeFilteredWithRatio.project(4).types(Double.class).sum(0).groupBy(0).reduceGroup(new CalculateAvgRatio()).withBroadcastSet(numVerticesFiltered, "numVertices");




      /* write results to a file */
      avgRatio.writeAsText(Config.outputPath(), FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  public static class EdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

    @Override
    public void flatMap(String s, Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        long target = Long.parseLong(tokens[1]);
        boolean isFriend = "+1".equals(tokens[2]);

        System.out.println("Source:" + source + " Target: " + target + " isFriend: " +isFriend);

        collector.collect(new Tuple3<Long, Long, Boolean>(source, target, isFriend));
      }
    }
  }

  public static class CountVertices implements GroupReduceFunction<Tuple1<Long>, Long> {
    @Override
    public void reduce(Iterable<Tuple1<Long>> vertices, Collector<Long> collector) throws Exception {

      System.out.println("BEGIN CountVertices");



      Long count = new Long(Iterables.size(vertices));

      System.out.println("Total number of vertices: " + count);



      collector.collect(count);

      System.out.println("STOP CountVertices");

    }
  }


  public static class DegreeOfFilteredFriendsAndFoesWithRatio implements GroupReduceFunction<Tuple2< Long, Boolean>, Tuple5<Long,Long, Long, Long, Double>> {

    @Override
    public void reduce(Iterable<Tuple2< Long, Boolean>> tuples, Collector<Tuple5<Long, Long, Long, Long, Double>> collector) throws Exception {

      System.out.println("BEGIN DegreeOfFilteredFriendsAndFoes");


      Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();

        long count = 1L;
        long friends = 0L;
        long foes = 0L;

        Tuple2<Long, Boolean> first = iterator.next();
        Long vertexId = first.f0;

        if(first.f1)
            friends++;
        else
            foes++;


        // get other values
      while (iterator.hasNext()) {

        Boolean isFriend = iterator.next().f1;

        if(isFriend){
            friends++;

        } else {
              foes++;

          }


        count++;
      }


        if (friends == 0 | foes == 0) {
        System.out.println("Skip because vertex:"+ vertexId +" + friends: "+friends+ " and foes: " +foes);
      }
      else {

            double ratio = (double) friends / foes;

          System.out.println("VertexId: " + vertexId + "  Degree: " +count + " Friends: " +friends + " Foes: " + foes + " Ratio: "+ratio );

          collector.collect(new Tuple5<Long, Long, Long, Long, Double>(vertexId, count, friends, foes, ratio));

      }


      System.out.println("STOP DegreeOfFilteredFriendsAndFoes");

    }
  }


  public static class CalculateAvgRatio extends RichGroupReduceFunction<Tuple1<Double>, Tuple1<Double>> {

    private long numVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple1<Double>> ratioSum, Collector<Tuple1<Double>> collector) throws Exception {

      System.out.println("BEGIN CalculateAvgRatio");

       Double avgRatio =  ratioSum.iterator().next().f0;

      System.out.println("Ratio Sum: " + avgRatio);
        System.out.println("Vertices #: " + numVertices);
        System.out.println("AvgRatio #: " + avgRatio/numVertices);


              collector.collect(new Tuple1<Double>(avgRatio / numVertices));

        System.out.println("END CalculateAvgRatio");

    }
  }





}
