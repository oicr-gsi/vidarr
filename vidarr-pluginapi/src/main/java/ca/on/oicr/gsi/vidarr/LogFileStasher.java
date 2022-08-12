package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.node.NullNode;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Allows workflow engines to store logs from failed jobs for inclusion in debugging information */
@JsonTypeIdResolver(LogFileStasher.LogFileStasherIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface LogFileStasher {
  final class LogFileStasherIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends LogFileStasher>> knownIds =
        ServiceLoader.load(LogFileStasherProvider.class).stream()
            .map(Provider::get)
            .flatMap(LogFileStasherProvider::types)
            .collect(Collectors.toMap(Pair::first, Pair::second));

    @Override
    public Id getMechanism() {
      return Id.CUSTOM;
    }

    @Override
    public String idFromValue(Object o) {
      return knownIds.entrySet().stream()
          .filter(known -> known.getValue().isInstance(o))
          .map(Entry::getKey)
          .findFirst()
          .orElseThrow();
    }

    @Override
    public String idFromValueAndType(Object o, Class<?> aClass) {
      return idFromValue(o);
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      final var clazz = knownIds.get(id);
      return clazz == null ? null : context.constructType(clazz);
    }
  }
  /** The type of log being stored */
  enum Kind {
    /** The log is the standard error output from a job */
    STDERR("stderr_stashed"),
    /** The log is the standard output from a job */
    STDOUT("stdout_stashed");

    private final String property;

    Kind(String property) {
      this.property = property;
    }

    /** The JSON property name where this output should be stored */
    public String property() {
      return property;
    }
  }

  /**
   * A mechanism to inform Vidarr/a workflow engine about the state of a log stashing operation
   *
   * <p>This is mostly a wrapper around {@link WorkMonitor} without state management
   */
  abstract class StashMonitor {

    private final WorkMonitor<?, ?> owner;

    /**
     * Create a new monitor
     *
     * @param owner the work monitor that will be used for scheduling
     */
    protected StashMonitor(WorkMonitor<?, ?> owner) {
      this.owner = owner;
    }

    /**
     * Indicate that the stashing is complete
     *
     * <p>Since log stashing is best effort, this can also be used to report an error. Either
     * provide a null value or a JSON object with detailed information.
     *
     * @param result the information that should be placed in the debugging information to locate
     *     the stashed log
     */
    public abstract void complete(JsonNode result);

    /**
     * Write something interesting
     *
     * @param level how important this message is
     * @param message the message to display
     */
    public final void log(Level level, String message) {
      owner.log(level, "Stashing log failed: " + message);
    }

    /**
     * Request that Vidarr schedule a callback at the next available opportunity
     *
     * <p>This cannot be called after {@link #complete(JsonNode)}
     *
     * @param task the task to execute
     */
    public final void scheduleTask(Runnable task) {
      owner.scheduleTask(task);
    }

    /**
     * Request that Vidarr schedule a callback at a specified time in the future
     *
     * <p>This scheduling is best-effort; Vidarr may execute a task earlier or later than requested
     * based on load and priority.
     *
     * <p>This cannot be called after {@link #complete(JsonNode)}
     *
     * @param delay the amount of time to wait; if delay is < 1, this is equivalent to {@link
     *     #scheduleTask(Runnable)}
     * @param units the time units to wait
     * @param task the task to execute
     */
    public final void scheduleTask(long delay, TimeUnit units, Runnable task) {
      owner.scheduleTask(delay, units, task);
    }
  }

  /** A log file stasher that discards the log file and completes with a null value. */
  LogFileStasher DISCARD =
      (vidarrId, monitor, logFile, labels) -> monitor.complete(NullNode.getInstance());

  /**
   * Write a log file to better storage
   *
   * @param vidarrId the Vidarr workflow run id
   * @param monitor a monitor to allow asynchronous execution
   * @param logFile the path to the log file
   * @param labels any extra labels that should be added to the log store, if applicable
   */
  void stash(String vidarrId, StashMonitor monitor, String logFile, Map<String, String> labels);
}
