package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import ca.on.oicr.gsi.vidarr.server.dto.TargetConfiguration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class DatabaseBackedTestConfiguration {

  private static final String vidarrTest = "vidarr-test";
  private static final String dbName = "vidarr-test";
  private static final String dbUser = "vidarr-test";
  private static final String dbPass = "vidarr-test";
  private static final TemporaryFolder unloadDirectory = new TemporaryFolder();

  public static JdbcDatabaseContainer getTestDatabaseContainer() {
    return new PostgreSQLContainer("postgres:12-alpine")
        .withDatabaseName(dbName)
        .withUsername(dbUser)
        .withPassword(dbPass);
  }

  protected static ServerConfiguration getTestServerConfig(
      JdbcDatabaseContainer pg, TemporaryFolder unloadDir, int port) {
    ServerConfiguration config = new ServerConfiguration();
    config.setName(pg.getContainerName());
    config.setDbHost(pg.getHost());
    config.setDbName(pg.getDatabaseName());
    config.setDbPass(pg.getPassword());
    config.setDbUser(pg.getUsername());
    config.setDbPort(pg.getFirstMappedPort());
    config.setPort(port);
    config.setUrl("http://localhost:" + port);
    config.setOtherServers(new HashMap<>());
    config.setInputProvisioners(Collections.singletonMap("raw", new RawInputProvisioner()));
    config.setWorkflowEngines(new HashMap<>());
    config.setOutputProvisioners(new HashMap<>());
    config.setRuntimeProvisioners(new HashMap<>());
    Map<String, TargetConfiguration> targets = new HashMap<>();
    TargetConfiguration target = new TargetConfiguration();
    target.setConsumableResources(List.of());
    target.setInputProvisioners(List.of());
    target.setOutputProvisioners(List.of());
    target.setRuntimeProvisioners(List.of());
    target.setWorkflowEngine("testEngine");
    targets.put("bullseye", target);
    config.setTargets(targets);
    config.setUnloadDirectory(unloadDir.getRoot().getAbsolutePath());
    return config;
  }

  protected static TemporaryFolder getUnloadDirectory() {
    return unloadDirectory;
  }
}
