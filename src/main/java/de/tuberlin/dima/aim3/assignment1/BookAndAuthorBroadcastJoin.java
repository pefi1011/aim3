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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import com.google.common.base.Joiner;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;


public class BookAndAuthorBroadcastJoin extends HadoopJob {

  private static final String AUTHORS_PATH = "minimumQuality";

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    //IMPLEMENT ME

    Job myJob = prepareJob(books, outputPath, TextInputFormat.class, BroadCastJoinMapper.class,
           Text.class, NullWritable.class, TextOutputFormat.class);



    // pass path param
    myJob.getConfiguration().set(AUTHORS_PATH, authors.toString());

    System.out.println(authors.toString());

    myJob.waitForCompletion(true);

    return 0;
  }

  static class BroadCastJoinMapper extends Mapper<Object, Text, Text, NullWritable> {

    private HashMap<String, String> authorHashMap = new HashMap<String, String>();


    @Override
    protected void map(Object key, Text value, Context ctx) throws IOException,
            InterruptedException {

      // get authors from the context
      String authorsPath = ctx.getConfiguration().get(AUTHORS_PATH);


      BufferedReader br = new BufferedReader(new FileReader(authorsPath));
      String currentLine;

      // transform author from string into hashMap
      while ((currentLine = br.readLine()) != null) {

        // tokanize after tab
        String authorDetails[] = currentLine.split("\t");

        authorHashMap.put(authorDetails[0], authorDetails[1]);
      }

      br.close();


      // tokenize book string after tab
      String[] books = value.toString().split("\t");


      System.out.println(books[0]);
      System.out.println(books[1]);
      System.out.println(books[2]);

      // according to data 1-n relationship
      // where book.authorId = author.id
      String authorId = authorHashMap.get(books[0]);
      if (authorId != null) {

        String out = Joiner.on('\t').join(new Object[] {
                authorId,
                books[2],
                books[1]});


        ctx.write(new Text(out), NullWritable.get());
      }

      //else {
       // System.out.println("Debug: No matching author found for id " + fields[0]);
     // }

    }
  }

}