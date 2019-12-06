import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class cleaner {
  public static void main(String[] args) {
    HashMap<String, String> works = new HashMap<>();
    BufferedReader br = null;
    try {
      Writer writer = null;
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("cleaned.txt"), StandardCharsets.UTF_8));
      br = new BufferedReader(new FileReader(args[0]));
      String line = null;

      while ((line = br.readLine()) != null) {
        if (!works.containsKey(line)) {
          works.put(line,"");
          writer.write(line + "\n");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
