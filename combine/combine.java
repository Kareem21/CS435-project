import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class combine {

  public static void main(String[] args) {
    try {
      FileSystem files = FileSystem.get(new Configuration());
      files.delete(new Path("/out"), true);
      files.delete(new Path("/BasJanAder"), true);
      Configuration conf = new Configuration();

  // PART A
      Job job1 = Job.getInstance(conf, "part 1");
      job1.setJarByClass(combine.class);
      job1.setMapperClass(Cm1.class);
      job1.setReducerClass(Cr1.class);
      job1.setNumReduceTasks(1);

      // Outputs from the Mapper.
      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(Text.class);

      // Outputs from Reducer.
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);

      // input output paths
      FileInputFormat.setInputPaths(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path("/BasJanAder"));
      job1.waitForCompletion(true);

  // PART B
      Job job2 = Job.getInstance(conf, "part 2");
      job2.setJarByClass(combine.class);
      job2.setMapperClass(Cm2.class);
      job2.setReducerClass(Cr2.class);
      job2.setNumReduceTasks(1);


      // Outputs from the Mapper.
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      // Outputs from Reducer.
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      // input output paths
      FileInputFormat.setInputPaths(job2, new Path("/BasJanAder"));
      FileOutputFormat.setOutputPath(job2, new Path(args[1]));
      job2.waitForCompletion(true);

      System.exit(job2.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }
  }
}