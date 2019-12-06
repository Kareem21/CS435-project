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
      if(!temp[0].contains("no"))
        if (Integer.parseInt(temp[0]) < low){
          low = Integer.parseInt(temp[0]);
        }
    }

    boolean flag = false;
    for (String cur : in) {
      cur = cur.replaceAll(" +", " ");
      String[] temp = cur.split(" ");
        if(temp.length>1) {
          String[] dim = temp[1].split(",");
          if(dim.length>1) {
            if (dim[0].length() > 0 && dim[1].length() > 0) {
              double lows = ((Integer.parseInt(temp[0]) - low));
              context.write(new Text( lows + "")
                  , new Text(Double.parseDouble(dim[0]) / Double.parseDouble(dim[1]) + ""));
              flag = true;
            }
          }
          if(!flag) {
            context.write(new Text("0000000000")
                , new Text("0000000000"));
          }
        }
    }

  }
}