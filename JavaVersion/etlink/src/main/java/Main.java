public class Main {

  public static void main(String[] args) {
    try {
      Extraction e = new Extraction("https://en.wikipedia.org/wiki/Napoleon");
      e.run();
    }
    catch (Exception e) {
      System.out.print(e);
    }
  }
}
