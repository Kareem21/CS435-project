import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {

      String k = "";
      String v = "";
      if (value.toString().contains(",support:")) {

        //GETS THE ARTIST NAME
        String[] input = value.toString().split("\"");
        if(input.length<1)
        k = input[1];

        //GETS THE ARTWORKS YEAR
        for (String i : input) {
          if(i.length() > 6) {
            if (i.charAt(0) == ',' && i.charAt(5) == ',') {
              v = i.substring(1, 6);
            }
          }
        }

        //GETS ARTWORKS DIMENSIONS
        String[] temp = value.toString().split(",support:");
        input = temp[1].split(" ");

        v += " " + input[1];

        context.write(new Text(k), new Text(v));
      }
    }
  }

