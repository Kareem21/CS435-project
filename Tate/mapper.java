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
      String[] i = sanit.split(",");
      name = i[2].replace("\"", "");
      name = name.replaceAll(" +", " ");
      String[] parts = name.split(" ");
      name = "";

      for (int n = 1; n < parts.length; n++) {
        name += parts[n] + " ";
      }
      name += parts[0];
      if (name.toLowerCase().contains("&")
          || name.toLowerCase().contains("and")
          || name.toLowerCase().contains("anonymous"))
      {
        name = "";
      }

      year = i[9];

      //GETS ARTWORKS DIMENSIONS
      String[] temp = value.toString().split(",support:");
      String dd = temp[1].replaceAll("[^0-9.)]", " ");
      dd = dd.replaceAll("[^0-9.)]", " ");
      dd = dd.replaceAll(" +", " ");
      if (dd.charAt(0) == ' '){
        dd = dd.substring(1);
      }
      i = dd.split(" ");
      if (i.length >=2) {
        dim = i[1] + " " + i[2];
        if (i[0].length() > 0 && i[1].length() > 0)
          if ((i[0].charAt(0) >= '0' && i[0].charAt(0) <= '9')
              && (i[1].charAt(0) >= '0' && i[1].charAt(0) <= '9')) {
            double w = Double.parseDouble(i[0]); // mm dimensions
            double h = Double.parseDouble(i[1]); // mm dimensions
            dim = " " + w + "," + h;
          }
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