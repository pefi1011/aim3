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

package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

  private static final String MINIMUM_QUALITY = "minimumQuality";

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    //IMPLEMENT ME
    Job avgTemperature = prepareJob(inputPath, outputPath, TextInputFormat.class, AverageTemperaturePerMonthMapper.class,
            Text.class, DoubleWritable.class, AverageTemperaturePerMonthReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);

    avgTemperature.getConfiguration().set(MINIMUM_QUALITY, Double.toString(minimumQuality));

    avgTemperature.waitForCompletion(true);


    return 0;
  }

  // map phase
  static class AverageTemperaturePerMonthMapper extends Mapper<Object,Text,Text,DoubleWritable> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      // IMPLEMENT ME

      // Load settings
      double minimumQuality = Double.parseDouble(
              ctx.getConfiguration().get(MINIMUM_QUALITY));

      // 1. tokenize lines
      String currentLine = line.toString();
      String[] currentLineSplitted = currentLine.split("\\s+");

      // 2. extract values from each line
      String year = currentLineSplitted[0];
      String month = currentLineSplitted[1];
      double temperature = Double.parseDouble(currentLineSplitted[2]);
      double quality = Double.parseDouble(currentLineSplitted[3]);


        System.out.print("Year :" + year + " ");
        System.out.print("Month :" + month + " ");
        System.out.print("Temperature :" + temperature + " ");
        System.out.print("Quality :" + quality + " ");

      // quality check
      if(quality >= minimumQuality) {

        // key year_month
        String myKey = year + "\t" + month;

          System.out.println(myKey + temperature);
        ctx.write(new Text(myKey), new DoubleWritable(temperature));



      }
    }
  }

  // reduce phase
  static class AverageTemperaturePerMonthReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
            throws IOException, InterruptedException {
      // IMPLEMENT ME

      double count = 0;
      double sumMonthTemperature = 0.0;

        // TODO ask why is while loop is not working
//      while (values.iterator().hasNext()) {
//
//        sumMonthTemperature = values.iterator().next().get();
//        count++;
//      }

        for (DoubleWritable temperature : values){
            sumMonthTemperature += temperature.get();
            count++;

        }

        System.out.println("Total temperature: " + sumMonthTemperature);
        System.out.println("Days: " + count);

      DoubleWritable avgTemperaturePerMonth = new DoubleWritable(sumMonthTemperature/count);

        System.out.println("Key: " + key + " Avg: " + avgTemperaturePerMonth.toString());
      ctx.write(key, avgTemperaturePerMonth);

    }



  }



}