package base;

public class Output {

  private OutputType type;
  private String path;

  public Output(OutputType type, String path) {
    this.type = type;
    this.path = path;
  }

  public OutputType getType() {
    return type;
  }

  public void setType(OutputType type) {
    this.type = type;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }
}
