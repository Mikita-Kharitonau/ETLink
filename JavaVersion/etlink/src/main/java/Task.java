import java.util.List;

public interface Task {

  List<Task> getDependencies();

  TaskResultStatus run();

  Output getOutput();

}
