package base;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SimpleTask implements Task {

  protected Properties appProps;
  protected String stageName;
  protected Logger logger;

  public List<Task> getDependencies() {
    return null;
  }

  public boolean resolveDependencies() {
    for (Task task : getDependencies()) {
      if (task.resolveAndRun() != TaskResultStatus.Success) {
        return false;
      }
    }
    return true;
  }

  public TaskResultStatus resolveAndRun() {
    if (resolveDependencies()) {
      return run();
    } else {
      // TODO ...
      return TaskResultStatus.Error;
    }
  }

  public TaskResultStatus run() {
    return TaskResultStatus.Unknown;
  }

  public Output getOutput() {
    return null;
  }

  protected Properties loadConfig() throws IOException {
    Properties appProps = new Properties();
    InputStream configIS = SimpleTask.class.getClassLoader().getResourceAsStream("app.properties");
    appProps.load(configIS);

    return appProps;
  }

  public Properties getAppProps() {
    return appProps;
  }

  public void setAppProps(Properties appProps) {
    this.appProps = appProps;
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public Logger getLogger() {
    return logger;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
  }
}
