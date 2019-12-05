import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context) throws
      IOException, InterruptedException {
    String name = "";   //index 14
    String year = "";   //index 20
    String dim  = "";   //index 25

    String intake = clean(value.toString());
    String[] input = intake.split(",");
    if(input.length > 25 &&  !input[14].equals("") && !input[25].equals("")) {
      if (input[24].toLowerCase().contains("oil")
          || input[24].toLowerCase().contains("canvas")
          || input[24].toLowerCase().contains("paint")
          || input[24].toLowerCase().contains("acrylic")
          || input[24].toLowerCase().contains("pastel")
          || input[24].toLowerCase().contains("stretched")
          || input[24].toLowerCase().contains("gouache")
          || input[24].toLowerCase().contains("on board"))
      {

  // NAME EXTRACT
        if (!input[14].contains("|")
            && !input[14].toLowerCase().contains("anonymous")
            && !input[14].toLowerCase().contains("&")
            && !input[14].toLowerCase().contains("and"))
        {
          name = input[14];
          name = name.replace('\"', ' ');
        }

  // DIMENSION EXTRACT
        dim = StringUtils.substringBetween(input[25], "(", ")");
        if (dim != null) {
          dim = dim.replaceAll("[^0-9.)]", " ");
          dim = dim.replaceAll(" +", " ");

          String[] i = dim.split(" ");
          // If dims are correct format
          if (i.length >= 2)
            if (i[0].length() > 0 && i[1].length() > 0)
              if ((i[0].charAt(0) >= '0' && i[0].charAt(0) <= '9') && (i[1].charAt(0) >= '0' && i[1].charAt(0) <= '9')) {
                double w = Double.parseDouble(((i[0]).replace(")", ""))) * 10; // mm dimensions
                double h = Double.parseDouble(((i[1]).replace(")", ""))) * 10; // mm dimensions
                dim = " " + w + "," + h;
          }
        } else dim = "";

  // YEAR EXTRACT
        if (!input[23].equals("")) {
          if (input[23].contains("|")) {
            year = input[23].split("\\|")[0];
          } else year = input[23];
        }
      }
    }

    if(!name.equals("") && !dim.equals("") && !year.equals(""))
      context.write(new Text(name), new Text(year + " " + dim));
  }

  // Removes commas inside quotes
  // Retains CSV delineation
  private String clean(String args) {
    StringBuilder copy = new StringBuilder();
    boolean inQuotes = false;

    for(int i=0; i<args.length(); ++i) {
      if (args.charAt(i)=='"')
        inQuotes = !inQuotes;
      if (args.charAt(i)==',' && inQuotes)
        copy.append(" ");
      else copy.append(args.charAt(i));
    }
    return copy.toString();
  }
}