import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.* ;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;

class Keyvalue {
  public int key ;
  public double value ;

  public Keyvalue(int k, double v) {
    key = k ;
    value = v ;
  }
}

class CustomComparator implements Comparator<Keyvalue> {
  @Override
  public int compare(Keyvalue key1, Keyvalue key2) {
    return key1.key < key2.key ? -1 : 1 ;
  }
}


public class csum extends Configured implements Tool {
  static ArrayList<Keyvalue> exclusive_csum ;

   /*
    *  step 1 of cumsum
    */
   public static class MapClass extends MapReduceBase implements Mapper< LongWritable, Text, Text, Text > {  
    
      public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
        String delims = "[(,) ]+";
        String[] tokens = value.toString().split(delims);
        String index = tokens [1] ;
        String partition = tokens [2] ;
        String output_value = "(" + index + "," + tokens [3] + ")" ;
        output.collect(new Text(partition), new Text(output_value)) ;
      }
    }
   /*
    *  A combine class that just emits the sum of the input values.
    */
    public static class Combine extends MapReduceBase implements Reducer< Text, Text, Text, Text > {

      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String delims = "[(,)]";
        double sum = 0 ;
        while (values.hasNext()) {
          String value = values.next().toString() ;
          String[] tokens = value.split(delims);
          sum += Double.parseDouble(tokens [2]) ;
        }
        String output_value = "(" + key.toString() + "," + Double.toString(sum) + ")" ;
        output.collect(new Text("1"), new Text(output_value));
      }
    }

    //A reduce class that computes the exclusive cumsum of the elements
    public static class Reduce extends MapReduceBase implements Reducer< Text, Text, Text, Text > {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String delims = "[(,)]";
        ArrayList<Keyvalue> list = new ArrayList<Keyvalue>() ;
        while (values.hasNext()) {
          String value = values.next().toString() ;
          String[] tokens = value.split(delims);
          list.add(new Keyvalue(Integer.parseInt(tokens [1]), Double.parseDouble(tokens [2]))) ;
        }
        //sort the list on partition ids
        Collections.sort(list, new CustomComparator()) ;
        double sum = 0 ;
        for (Keyvalue k : list) {
          output.collect(new Text(Integer.toString(k.key)), new Text(Double.toString(sum)));
          sum = sum + k.value ; 
        }
      }
    }

    public static class MapClassTwo extends MapReduceBase implements Mapper< LongWritable, Text, Text, Text > {
    
      public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
        String delims = "[(,) ]+";
        String[] tokens = value.toString().split(delims);
        String index = tokens [1] ;
        String partition = tokens [2] ;
        String output_value = "(" + index + "," + tokens [3] + ")" ;
        output.collect(new Text(partition), new Text(output_value)) ;
      }
    }

    public static class CombineTwo extends MapReduceBase implements Reducer< Text, Text, Text, Text > {

      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String delims = "[(,)]";
        ArrayList<Keyvalue> list = new ArrayList<Keyvalue>() ;
        while (values.hasNext()) {
          String value = values.next().toString() ;
          String[] tokens = value.split(delims);
          list.add(new Keyvalue(Integer.parseInt(tokens [1]), Double.parseDouble(tokens [2]))) ;
        }
        //traverse the list in ascending or descending order of the indices
        double sum = exclusive_csum.get(Integer.parseInt(key.toString()) - 1).value ;
        if (list.size() > 1 && list.get(0).key < list.get(1).key) {
          for (Keyvalue k : list) {
            sum = sum + k.value ; 
            String output_value = "(" + Integer.toString(k.key) + "," + Double.toString(sum) + ")" ;
            output.collect(key, new Text(output_value));
          }
        } else {
          for (int i = list.size() - 1 ; i >= 0 ; i--) {
            Keyvalue k = list.get(i) ;
            sum = sum + k.value ;
            String output_value = "(" + Integer.toString(k.key) + "," + Double.toString(sum) + ")" ;
            output.collect(key, new Text(output_value));
          }
        }
      }
    }

    static int printUsage() {
      System.out.println("csum [-m #mappers ] [-r #reducers] input_file output_file");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

     /*
      * Create output files based on the output record's key name.
      */
    static class KeyBasedMultipleTextOutputFormat
      extends MultipleTextOutputFormat<Text, Text> {
      @Override
      protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        return key.toString() + "_" + name;
      }
    }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), csum.class);
    conf.setJobName("csum");

    // the keys and values are text
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapClass.class);
    // Here we set the combiner!!!! 
    conf.setCombinerClass(Combine.class);
    conf.setReducerClass(Reduce.class);

    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {     
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
        args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 3) {
      System.out.println("ERROR: Wrong number of parameters: " +
      other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    JobClient.runJob(conf);
    
    exclusive_csum = new ArrayList<Keyvalue>() ; 
    Configuration config = getConf() ;
    //config.addResource(new Path(other_args.get(1))) ;
    FileSystem fSystem = FileSystem.get(config);
    if (fSystem == null) {
      System.out.println("Problem getting FileSystem");
    }
    BufferedReader reader = new BufferedReader( new InputStreamReader (fSystem.open(new Path("csum/output/part-00000"))));
    System.out.println("Reader successfully initialized");
    String line ;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
      String[] tokens = line.split("\\t", -1);
      System.out.println(tokens.length);
      exclusive_csum.add(new Keyvalue(Integer.parseInt(tokens [0]), Double.parseDouble(tokens [1]))) ;
    }

    //step two
    JobConf confTwo = new JobConf(getConf(), csum.class);
    confTwo.setJobName("csum2");

    // the keys and values are text
    confTwo.setOutputKeyClass(Text.class);
    confTwo.setOutputValueClass(Text.class);

    confTwo.setMapperClass(MapClassTwo.class);
    // Here we set the combiner!!!! 
    confTwo.setCombinerClass(CombineTwo.class);

    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          confTwo.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {     
          confTwo.setNumReduceTasks(Integer.parseInt(args[++i]));
        } 
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
        args[i-1]);
        return printUsage();
      }
    }
 
    confTwo.setOutputFormat(KeyBasedMultipleTextOutputFormat.class);
    FileInputFormat.setInputPaths(confTwo, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(confTwo, new Path(other_args.get(2)));

    JobClient.runJob(confTwo);
    
    return 0;
  }
             
             
  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(new Configuration(), new csum(), args);
    System.exit(res);
  }
}
