import base.SimpleTask;
import base.Task;
import base.TaskResultStatus;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DbUpload extends SimpleTask {

  private final List<String> urls;

  public DbUpload(List<String> urls) throws IOException {
    stageName = "dbupload";
    logger = Logger.getLogger(Extraction.class);

    this.urls = urls;
    try {
      appProps = loadConfig();
    } catch (IOException e) {
      logger.error("Can't initialize DbUpload task: can't load config");
      throw e;
    }
  }

  @Override
  public List<Task> getDependencies() {
    ArrayList<Task> dependencies = new ArrayList<>();
    try {
      dependencies.add(new Saving(urls));
    } catch (IOException e) {
      logger.warn("WARNING! Skipping Saving(");
      urls.forEach(url -> logger.warn(url));
      logger.warn(") task...");
    }
    return dependencies;
  }

  @Override
  public TaskResultStatus run() {
    try {
      SparkConf conf = new SparkConf().setMaster("local").setAppName("ETLink dbupload");

      JavaSparkContext sc = new JavaSparkContext(conf);

      SQLContext sqlContext = new SQLContext(sc);

      Dataset<Row> df = sqlContext.read().parquet(getDependencies().get(0).getOutput().getPath());
      df.show(20, false);

      // TODO: create config variables for host, port, etc.
      String url = "jdbc:mysql://localhost:3306/etlink_java";
      Properties props = new Properties();
      props.put("user", "etlink");
      props.put("password", "etlink");
      df.write().mode(SaveMode.Append).jdbc(url, "counted_links", props);

      sc.stop();

      return TaskResultStatus.Success;
    } catch (Exception e) {
      logger.error(e);
      return TaskResultStatus.Error;
    }
  }


}
