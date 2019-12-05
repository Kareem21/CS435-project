import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Cr1 extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    ArrayList<String> in = new ArrayList<>();
    int low = Integer.MAX_VALUE;
    for (Text cur : values) {
      in.add(cur.toString());
      String[] temp = cur.toString().split(" ");
      if (Integer.parseInt(temp[0]) < low){
        low = Integer.parseInt(temp[0]);
      }
    }

    for (String cur : in) {
        String[] temp = cur.toString().split("  ");
        String[] dim = temp[1].replaceAll(",", " ").split(" ");
        dim[0] = dim[0].replaceAll(".0", "").replaceAll("[^0-9.)]", " ");
        dim[1] = dim[1].replaceAll(".0", "").replaceAll("[^0-9.)]", " ");
        dim[0] = dim[0].replaceAll(".", "").replaceAll("[^0-9.)]", " ");
        dim[1] = dim[0].replaceAll(".", "").replaceAll("[^0-9.)]", " ");

        if (dim.length >= 2 && dim[0].length() > 0 && dim[1].length() > 0) {
          context.write(new Text((Integer.parseInt(temp[0]) - low + "")), new Text(Double.parseDouble(dim[0]) / Double.parseDouble(dim[1]) + ""));
        }
      }
  }
}