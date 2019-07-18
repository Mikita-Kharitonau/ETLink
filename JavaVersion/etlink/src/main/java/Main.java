import base.TaskResultStatus;

import java.util.ArrayList;
import java.util.List;

public class Main {

  public static void main(String[] args) {

    List<String> urls = new ArrayList<>();
    urls.add("https://en.wikipedia.org/wiki/Napoleon");
    urls.add("https://en.wikipedia.org/wiki/Napoleonic_Wars");
    urls.add("https://en.wikipedia.org/wiki/United_Kingdom_of_Great_Britain_and_Ireland");
    urls.add("https://en.wikipedia.org/wiki/British_Empire");
    urls.add("https://en.wikipedia.org/wiki/League_of_Nations_mandate");
    urls.add("https://en.wikipedia.org/wiki/League_of_Nations");
    urls.add("https://en.wikipedia.org/wiki/Arbitration");
    urls.add("https://en.wikipedia.org/wiki/Non-binding_arbitration");
    urls.add("https://en.wikipedia.org/wiki/Queen%27s_Counsel");

    try {
      DbUpload dbUpload = new DbUpload(urls);
      TaskResultStatus result = dbUpload.resolveAndRun();
      System.out.println("### Execution Summary ###");
      System.out.println(result);
      System.out.println("#########################");
    }
    catch (Exception e) {
      System.out.print(e);
    }
  }
}
