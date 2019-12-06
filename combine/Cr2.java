import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Cr2 extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int cnt    = 0;
    double sum = 0;
    double res = 0;
    for (Text cur : values) {
      sum += Double.parseDouble(cur.toString());
      cnt++;
    }
    res = sum /cnt;
    if (key == new Text("0000000000")){
      context.write(key, new Text(cnt +""));
    }
    else if(cnt > 20) {
      context.write(key, new Text(res + ""));
    }
  }
}