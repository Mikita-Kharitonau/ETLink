package base;

import java.util.List;

public interface Task {

  List<Task> getDependencies();

  boolean resolveDependencies();

  TaskResultStatus resolveAndRun();

  TaskResultStatus run();

  Output getOutput();

}
