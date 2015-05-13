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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.base.Joiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    // IMPLEMENT ME

    // Useful stackoverflow: http://stackoverflow.com/questions/14728480/what-is-the-use-of-grouping-comparator-in-hadoop-map-reduce
    // composite key strategy, i.e. secondary sorting

    // instead of prepareJob we have to do the shitty work by ourself
    // because MultipleInputs


    // I Create job
    Job myJob = new Job(new Configuration(getConf()), "ReducerJoin");
    Configuration jobConf = myJob.getConfiguration();




    // II Set Map properties

    // 2. run myJob on book data that is TextInputFormat and give me AuthorJoinMapper as output
    MultipleInputs.addInputPath(myJob, books, TextInputFormat.class, BooksJoinMapper.class);
    // 1. run myJob on authors data that is TextInputFormat and give me AuthorJoinMapper as output
    MultipleInputs.addInputPath(myJob, authors, TextInputFormat.class, AuthorJoinMapper.class);

    // define data type of map's key
    myJob.setMapOutputKeyClass(TextPairKey.class);
    // define data type of map's value
    myJob.setMapOutputValueClass(Text.class);


    jobConf.setBoolean("mapred.compress.map.output", true);


    /// III Set Reduce properties

    // define reduce task
    myJob.setReducerClass(JoinReducer.class);
    // define data type of reducer's "map"
    // define data type of reducer's key
    myJob.setOutputKeyClass(Text.class);
    // define data type of reducer's value
    myJob.setOutputValueClass(NullWritable.class);


    // IV use self defined natural key partitioner and natural key group comparator
    // custom classes for secondary sorting
    myJob.setPartitionerClass(NaturalKeyPartitioner.class);
    myJob.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);


    // V set where to save the result

    // code from prepareJob - lines 87 and 88
    myJob.setOutputFormatClass(TextOutputFormat.class);
    jobConf.set("mapred.output.dir", outputPath.toString());


    myJob.waitForCompletion(true);

    return 0;
  }

  static abstract class JoinMapper extends Mapper<Object, Text, TextPairKey, Text> {

    /**
     * @return Return the data source, i.e. books or authors
     */
    abstract String getDataSource();

    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException,
            InterruptedException {

      // value in first round: books.tsv
      // value in second round: authors.tsv
      // split string (both books and authors) after tab
      String[] fields = value.toString().split("\t");


      System.out.println("Start Mapper");
      System.out.println("Length: : "+fields.length + "; Values: " + fields[0] + " " + fields[1]);


      // build composite key
      TextPairKey keyOut = new TextPairKey(new Text(fields[0]), new Text(getDataSource()));

      System.out.println("TextPairKey:" + keyOut.toString());

      System.out.println( keyOut.toString() + "; " + value);

      // Transmit the tuple data as value
      // For simplicity we send the author-id again, which adds minor traffic
      context.write(keyOut, value);

    }
  }

  static class JoinReducer extends Reducer<TextPairKey, Text, Text, NullWritable> {

    private Text out = new Text();  // single instance, to reduce allocations

    @Override
    protected void reduce(TextPairKey key, Iterable<Text> values,
                          Context context) throws IOException,
            InterruptedException {

      System.out.println("Start Reducer");

      // because of secondary sorting
      // first result is author
      // and all other are books

      Iterator<Text> iterator = values.iterator();

      String nextValue = iterator.next().toString();

      System.out.println("Composite key:" + key.id + " " + key.datasource + " ------> " + nextValue);

      String author = nextValue.split("\t")[1];

      // Iterate through book values.
      // Because of secondary sorting is every value a match.
      while (iterator.hasNext()) {


        Text newNextValue = iterator.next();
        System.out.println("Composite key:" + key.id + " " + key.datasource + " ------> " + newNextValue);
        String[] bookFields = newNextValue.toString().split("\t");


        String[] joinString = new String[] { author, bookFields[2], bookFields[1]};
        System.out.println("JoinString: "+joinString[0] + " " + joinString[1] + " " + joinString[2]);


        out.set(Joiner.on("\t").join(joinString));
        context.write(out, NullWritable.get());
      }
    }

  }


  static class AuthorJoinMapper extends JoinMapper {

    private static final String DATASOURCE = "author";

    @Override
    String getDataSource() { return DATASOURCE; }

  }

  static class BooksJoinMapper extends JoinMapper {

    private static final String DATASOURCE = "book";

    @Override
    String getDataSource() { return DATASOURCE; }

  }

  /**
   *  my composite key
   *  The composite key comparator will perform the sorting of the keys (and thus values)
   */
  static class TextPairKey implements WritableComparable<TextPairKey> {

    private Text id;
    private Text datasource;

    TextPairKey() {
      this.id = new Text();
      this.datasource = new Text();
    }

    TextPairKey(final Text id, final Text datasource) {
      this.id = id;
      this.datasource = datasource;
    }

    @Override
    public int compareTo(TextPairKey other) {
      // This will compare both parts of the composite key, for purpose of sorting
      int result = this.id.compareTo(other.getId());
      if (result == 0) {
        result = this.datasource.compareTo(other.getDatasource());
      }
      return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      id.write(out);
      datasource.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      datasource.readFields(in);
    }

    @Override
    public int hashCode() {
      // Considers both parts of the composite key
      // Default partitioner will send each century/title combination to a single reducer
      String combined = id.toString() + datasource.toString();
      return combined.hashCode();
    }

    @Override
    public String toString() {
      return id + "\t" + datasource;
    }

    private Text getId() {
      return id;
    }

    private Text getDatasource() {
      return datasource;
    }
  }



  /**
   *  Groups values together according to the natural key. We consider only the natural
   *  key, i.e. the id
   *  The natural key grouping comparator will group values based on the natural key
   */
  static class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
      super(TextPairKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

      TextPairKey a1 = (TextPairKey) a;
      TextPairKey b1 = (TextPairKey) b;
      return a1.getId().toString().compareTo(b1.getId().toString());
    }

  }

  /**
   * same as in natural key grouping comparator the natural key partioner considers
   * only the natural key, i.e. id
   * The natural key partitioner will send values with the same natural key to the same reducer.
   */
  static class NaturalKeyPartitioner extends Partitioner<TextPairKey, Text> {

    @Override
    public int getPartition(TextPairKey pair, Text value, int numPartitions) {
      int hashCode = pair.getId().hashCode();
      return hashCode % numPartitions;
    }

  }




}