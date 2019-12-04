import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class job {

  public static void main(String[] args) {
    try {
      FileSystem files = FileSystem.get(new Configuration());
      files.delete(new Path("/out"), true);
      Configuration conf = new Configuration();

  // TATE DATASET
      Job job = Job.getInstance(conf, "part 1");
      job.setJarByClass(job.class);
      job.setMapperClass(mapper.class);
      job.setReducerClass(reducer.class);

      // Outputs from the Mapper.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      // Outputs from Reducer.
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // input output paths
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.waitForCompletion(true);
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }
  }
}