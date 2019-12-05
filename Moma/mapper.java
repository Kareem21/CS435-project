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
    String dim  = "";
    double w;
    double h;
    String sanitized = clean(value.toString());

    String[] input = sanitized.split(",");
    if(input.length>10) {
      name = input[1].replace("\"", "");

      if (input[8].length() >= 4) {
        year = input[8];
        year = year.replaceAll("[^0-9\\-]", "");
        if (year.length() == 6) {
          year = year.substring(0, 3) + year.substring(5, 6);
        }
      }
      if (year.length() != 4) {
        year = findDate(input[8]);
      }
      String[] t = input[10].split("\\(");
      for (String cur : t) {
        if (cur.toLowerCase().contains("cm")) {
          cur = cur.replaceAll("[^0-9.)]", " ");
          cur = cur.replaceAll(" +", " ");
          String[] i = cur.split(" ");
          try {
            if (i.length >= 2 && cur.contains(")")) {
              if (i[0].length() > 0 && i[1].length() > 0)
                if ((i[0].charAt(0) >= '0' && i[0].charAt(0) <= '9') && (i[1].charAt(0) >= '0' && i[1].charAt(0) <= '9')) {
                  w = Double.parseDouble(cleanNum((i[0]).replace(")", ""))) * 10;
                  h = Double.parseDouble(cleanNum((i[1]).replace(")", ""))) * 10;
                  dim = " " + w + "," + h;
                }
            }
          } catch (NullPointerException e) {
            e.printStackTrace();
          }
        }
      }
      if (input[9].toLowerCase().contains("oil") || input[9].toLowerCase().contains("canvas") || input[9].toLowerCase().contains("paint") || input[9].toLowerCase().contains("acrylic")){
        if (!name.equals("") && !year.equals("") && !dim.equals(""))
          context.write(new Text(name), new Text(year + " " + dim));
      }
    }
  }



  private String findDate(String args)
  {
    int cnt = 0;
    StringBuilder date = new StringBuilder();

    for(int i=0; i<args.length(); ++i) {
      if(cnt == 4){
        return date.toString();
      }
      if (args.charAt(i) >= '0' && args.charAt(i) <= '9'){
        date.append(args.charAt(i));
        cnt++;
      }
      else {
        date = new StringBuilder();
        cnt = 0;
      }
    }

    System.out.println(args);
    return "";
  }


  private String clean(String args)
  {
    StringBuilder copy = new StringBuilder();

    boolean inQuotes = false;

    for(int i=0; i<args.length(); ++i) {
      if (args.charAt(i)=='"')
        inQuotes = !inQuotes;
      if (args.charAt(i)==',' && inQuotes)
        copy.append("");
      else
        copy.append(args.charAt(i));
    }

    System.out.println(args);
    return copy.toString();
  }

  private String cleanNum(String args)
  {
    StringBuilder copy = new StringBuilder();

    boolean num = false;
    boolean dec = false;

    for(int i=0; i<args.length(); ++i) {
      if( args.charAt(i) >= '0' && args.charAt(i) <= '9' && !num) {
        num = true;
      }
      if( !((args.charAt(i) >= '0' && args.charAt(i) <= '9') || args.charAt(i) >= '.') && num) {
        num = false;
        dec = false;
      }
      if(args.charAt(i) == '.') {
        if (args.charAt(i) == '.' && !dec && num) {
          dec = true;
          copy.append(".");
        }
      } else copy.append(args.charAt(i));
    }

    System.out.println(args);
    return copy.toString();
  }
}

