import base.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Saving extends SimpleTask {

  private final List<String> urls;

  public Saving(List<String> urls) throws IOException {
    stageName = "saving";
    logger = Logger.getLogger(Extraction.class);

    this.urls = urls;
    try {
      appProps = loadConfig();
    } catch (IOException e) {
      logger.error("Can't initialize Saving task: can't load config");
      throw e;
    }
  }

  @Override
  public List<Task> getDependencies() {
    ArrayList<Task> dependencies = new ArrayList<>();
    for (String url : urls) {
      try {
        dependencies.add(new Extraction(url));
      } catch (IOException e) {
        logger.warn("WARNING! Skipping Extraction(" + url + ") task...");
      }
    }
    return dependencies;
  }

  @Override
  public TaskResultStatus run() {
    try {
      SparkConf conf = new SparkConf().setMaster("local").setAppName("ETLink saving");

      JavaSparkContext sc = new JavaSparkContext(conf);
      SparkSession ss = SparkSession.builder().config(conf).getOrCreate();

      JavaRDD<String> overallRdd = sc.emptyRDD();
      for (Task dependency : getDependencies()) {
        overallRdd = overallRdd.union(fromInputToRdd(sc, dependency.getOutput()));
      }

      JavaPairRDD<String, Integer> countedLinks = overallRdd
                                    .mapToPair(link -> new Tuple2<>(link, 1))
                                    .reduceByKey(Integer::sum);

      List<StructField> fields = new ArrayList<>();
      fields.add(DataTypes.createStructField("link", DataTypes.StringType, false));
      fields.add(DataTypes.createStructField("count", DataTypes.IntegerType, false));
      StructType schema = DataTypes.createStructType(fields);

      JavaRDD<Row> rowRDD = countedLinks.map(record -> RowFactory.create(record._1, record._2));

      Dataset<Row> df = ss.createDataFrame(rowRDD, schema);

      df.write().parquet(getOutput().getPath());

      sc.stop();
      ss.stop();

      return TaskResultStatus.Success;

    } catch (Exception e) {
      logger.error(e);
      return TaskResultStatus.Error;
    }
  }

  public JavaRDD<String> fromInputToRdd(JavaSparkContext sc, Output input) {
    return sc.textFile(input.getPath());
  }

  @Override
  public Output getOutput() {
    final String fsname = appProps.getProperty("hdfsPrefix");
    final String username = System.getProperty("user.name");
    final String appName = appProps.getProperty("appName");
    final String appLang = appProps.getProperty("appLang");

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    final String dateTime = sdf.format(new Date(System.currentTimeMillis()));

    final String stage = stageName;
    final String filename = "saving";

    final String outputPath =
        String.format(
            "%s/users/%s/%s/%s/%s/%s/%s",
            fsname, username, appName, appLang, dateTime, stage, filename);

    return new Output(OutputType.HDFS, outputPath);
  }
}
