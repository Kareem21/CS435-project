import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {

    private boolean medium(String medium)
    {
        return(medium.contains("oil") || (medium.contains("canvas")) || medium.contains("paint") || medium.contains("acrylic") );
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String name = "";   //index  14 comes in as string
        String dim = "";    //index 25 can be "Dimensions unavailable"  //dimensions come in like this "37 1/8 x 21 x 20 3/4 in. (94.3 x 53.3 x 52.7 cm)"
        String year = "";   //index 20


        String sanit = clean(value.toString());
        String[] input = sanit.split(",");

        //ensure the line you're getting is a complete one by making sure length is >43
        if(input.length > 43 &&  !input[14].equals("") && !input[25].equals("")) {
            if(input[14].contains("|"))
            {
                name = input[14].split("\\|")[0];
            }
            else { name = input[14]; }
            char ch='"';
            name = name.replace(ch, ' ' );
            dim = StringUtils.substringBetween(input[25], "(", ")"); //gets the cm dimensions



            //if no dimensions were provided?
            if(dim == null) { dim = " "; }

            if(medium (input[24]) ) //ensuring the medium is valid
            {

                dim = dim.replace("x", ",");
                System.out.println("dim after regex " + dim);
            }
            //  if (dim.contains("x")) {
         //       String dim1 =  Integer.toString( (Integer.parseInt(dim.split("x")[0]) * 10) )   ; //convert to milimeters
         //       String dim2 = Integer.toString ( (Integer.parseInt(dim.split("x")[1]) * 10) );
         //       dim = dim1 + "," + dim2;
         //    }
            //Take only the first "End year" since some paintings have multiple "End years"
            if (!input[23].equals("")) {
                if(input[23].contains("|"))
                {
                    year = input[23].split("\\|")[0];
                }
                else { year = input[23]; }
            }
          // if(!year.equals("") && !dim.equals("") && medium(input[24]) )  //ensure year not empty
          //      System.out.println(name + " " + year + " " +  dim );
        };
        if(!name.equals("") && !dim.equals("")   && !year.equals("") && !name.equals(null) && !dim.equals(null)   && !year.equals(null)  )
            context.write(new Text(name), new Text(year + " " + dim));
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
        return copy.toString();
    }
    public static void main(String[]   args) throws Exception {
    }
}

