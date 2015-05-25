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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class SignedOutDegreeDistribution {

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

    /* Zahle wie viele ein vertex Beziehung hat und wie viel davon Freunde und Feinde sind*/
    DataSet<Tuple4<Long, Long, Long, Long>> verticesWithFriendsAndFoesDegree =
        edges.project(0, 2).types(Long.class, Boolean.class)
             .groupBy(0, 1).reduceGroup(new DegreeOfFriendsAndFoes());


    /* Compute the friends distribution */
    DataSet<Tuple2<Long, Double>> friendsDistribution =
            verticesWithFriendsAndFoesDegree.groupBy(2).reduceGroup(new DistributionElements())
                    .withBroadcastSet(numVertices, "numVertices");


    /* Compute the foes distribution */
    DataSet<Tuple2<Long, Double>> foesDistribution =
            verticesWithFriendsAndFoesDegree.groupBy(3).reduceGroup(new DistributionElements())
                    .withBroadcastSet(numVertices, "numVertices");


      /* join friends and foes resutls */
      DataSet<Tuple3<Long, Double, Double>> friendsAndFoesDistribution = friendsDistribution.join(foesDistribution).where(0).equalTo(0).projectFirst(0,1).projectSecond(1).types(Long.class,Double.class,Double.class);

      /* write results to a file */
      friendsAndFoesDistribution.writeAsText(Config.outputPath(), FileSystem.WriteMode.OVERWRITE);

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


  public static class DegreeOfVertex implements GroupReduceFunction<Tuple1<Long>, Tuple2<Long, Long>> {
    @Override
    public void reduce(Iterable<Tuple1<Long>> tuples, Collector<Tuple2<Long, Long>> collector) throws Exception {

      System.out.println("BEGIN DegreeOfVertex");


      Iterator<Tuple1<Long>> iterator = tuples.iterator();

      Long vertexId = iterator.next().f0;

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      System.out.println("VertexId: " + vertexId + " Degree: " +count);


      collector.collect(new Tuple2<Long, Long>(vertexId, count));

      System.out.println("STOP DegreeOfVertex");

    }

  }

  public static class DegreeOfFriendsAndFoes implements GroupReduceFunction<Tuple2< Long, Boolean>, Tuple4<Long,Long, Long, Long>> {

    @Override
    public void reduce(Iterable<Tuple2< Long, Boolean>> tuples, Collector<Tuple4<Long, Long, Long, Long>> collector) throws Exception {

      System.out.println("BEGIN DegreeOfFriendsAndFoes");


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

        if(isFriend)
          friends++;
        else
        foes++;


        count++;
      }

      System.out.println("VertexId: " + vertexId + "  Degree: " +count + " Friends: " +friends + " Foes: " + foes);


      collector.collect(new Tuple4<Long, Long, Long, Long>(vertexId, count, friends, foes));

      System.out.println("STOP DegreeOfFriendsAndFoes");

    }
  }

  public static class DistributionElement extends RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private long numVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> verticesWithDegree, Collector<Tuple2<Long, Double>> collector) throws Exception {

      System.out.println("BEGIN DistributionElement");


      Iterator<Tuple2<Long, Long>> iterator = verticesWithDegree.iterator();
      Long degree = iterator.next().f1;

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      System.out.println("Degree: " + degree + " Distribution: " + (double) count / numVertices);

      collector.collect(new Tuple2<Long, Double>(degree, (double) count / numVertices));

      System.out.println("END DistributionElement");

    }
  }

  public static class DistributionElements extends RichGroupReduceFunction<Tuple4<Long, Long, Long, Long>, Tuple2<Long, Double>> {

    private long numVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple4<Long, Long, Long, Long>> verticesWithFriendsAndFoesDegree, Collector<Tuple2<Long, Double>> collector) throws Exception {

      System.out.println("BEGIN DistributionElements");


      Iterator<Tuple4<Long, Long, Long, Long>> iterator = verticesWithFriendsAndFoesDegree.iterator();

        Tuple4<Long, Long, Long, Long> first = iterator.next();


        Long degree = first.f1;

        // TODO Ask why is summed, e.g. 341 158 0 158 and not 341 158 4 154
        System.out.println(first.f0 + " "+ first.f1+ " "+first.f2+ " "+first.f3);

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

     // System.out.println("Degree: " + degree + " Distribution: " + (double) count / numVertices);

      collector.collect(new Tuple2<Long, Double>(degree, (double) count / numVertices));

      System.out.println("END DistributionElements");

    }
  }

}
