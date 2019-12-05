import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws
      IOException, InterruptedException {

    String name = "";
    String year = "";
    String dim = "";

    if (value.toString().contains(",support:")) {
      String sanit = clean(value.toString());
      //GETS THE ARTIST NAME
      String[] input = sanit.split(",");
      if(input.length>1)
        name = input[2].replace("\"", "");
      year = input[9];

      //GETS ARTWORKS DIMENSIONS
      String[] temp = value.toString().split(",support:");
      String dd = temp[1].replaceAll("[^0-9.)]", " ");
      dd = dd.replaceAll("[^0-9.)]", " ");
      dd = dd.replaceAll(" +", " ");
      input = dd.split(" ");
      if (input.length > 3) {
        dim += " " + input[1] + "," + input[2];
      }

      if(!name.equals("") && !dim.equals("") && !year.equals(""))
        context.write(new Text(name), new Text(year + " " + dim));
    }
  }

  private String clean(String args)
  {
    StringBuilder copy = new StringBuilder();

    boolean inQuotes = false;

    for(int i=0; i<args.length(); ++i)
    {
      if (args.charAt(i)=='"')
        inQuotes = !inQuotes;
      if (args.charAt(i)==',' && inQuotes)
        copy.append(" ");
      else
        copy.append(args.charAt(i));
    }

    System.out.println(args);
    return copy.toString();
  }
}
