import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class leastN {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private HashMap<String, Integer> wordMap = new HashMap<String, Integer>();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      // word count
      while (itr.hasMoreTokens()) {
        String word = itr.nextToken();
        if (wordMap.containsKey(word)) {
          int count = wordMap.get(word); // get word count
          wordMap.put(word, count + 1);  // add one to the count
        }else {
          wordMap.put(word, 1);
        }
      }
    }
    
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException{
      // sort using tree map, after sorting: (count, [word1, word2, ...])
      Set<String> keysets = wordMap.keySet();
      for (String w : keysets) {
        int count = wordMap.get(w);
        context.write(new Text(w), new IntWritable(count));
      }
    }
  }
  
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
   private TreeMap<Integer, ArrayList<String>> tmap = new TreeMap<Integer, ArrayList<String>>();
   public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
     int sum = 0;
     for (IntWritable val : values) {
       sum += val.get();
     }

     // sort using tree map, after sorting: (count, [word1, word2, ...])
     if (tmap.containsKey(sum)) {
       tmap.get(sum).add(key.toString());
     } else {
       tmap.put(sum, new ArrayList<String>(Arrays.asList(key.toString())));
     }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
      for (int i = 0 ; i < 5; i++) {
        int count = tmap.firstKey();
        ArrayList<String> words = tmap.get(count);
        for (String w : words) {
          context.write(new Text(w), new IntWritable(count));
        }
        tmap.remove(count);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "leastN");
    job.setJarByClass(leastN.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}