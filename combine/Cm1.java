import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Cm1 extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws
      IOException, InterruptedException {

    String[] in = (value.toString()).split("\t");
    context.write(new Text(in[0]), new Text(in[1]));
  }
}