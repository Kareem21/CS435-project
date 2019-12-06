import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

public class sort {
  public static void main(String[] args) {
    HashMap<String, String> works = new HashMap<>();

    try {
      BufferedReader br = new BufferedReader(new FileReader(args[0]));
      String line;

      while ((line = br.readLine()) != null) {
        if (!works.containsKey(line.split("\t")[0])) {
          works.put(line.split("\t")[0].replace(".0", ""), line);
        }
      }

      PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
      for(int i = 0; i < 100; i++){
        if(works.containsKey(i +"")) {
          writer.println(works.get(i + ""));
          System.out.println(works.get(i + ""));
        }
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
