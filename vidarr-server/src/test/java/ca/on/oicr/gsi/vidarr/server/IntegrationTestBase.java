package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.defaultParser;
import static org.junit.Assert.*;

import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.RestAssured;
import io.restassured.parsing.Parser;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.TimeZone;
import org.flywaydb.core.Flyway;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

// Contains common attributes and methods needed for an integration test
public class IntegrationTestBase {
  @ClassRule
  public static JdbcDatabaseContainer pg =
      new PostgreSQLContainer("postgres:12-alpine")
          .withDatabaseName("vidarr-test")
          .withUsername("vidarr-test")
          .withPassword("vidarr-test");

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static Main main;
  protected static ServerConfiguration config;
  protected static final HttpClient CLIENT =
      HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

  @ClassRule public static final TemporaryFolder unloadDirectory = new TemporaryFolder();

  private static ServerConfiguration getTestServerConfig(GenericContainer pg) {
    ServerConfiguration config = new ServerConfiguration();
    config.setName("vidarr-test");
    config.setDbHost(pg.getHost());
    config.setDbName("vidarr-test");
    config.setDbPass("vidarr-test");
    config.setDbUser("vidarr-test");
    config.setDbPort(pg.getFirstMappedPort());
    config.setPort(8999);
    config.setUrl("http://localhost:" + config.getPort());
    config.setOtherServers(new HashMap<>());
    config.setInputProvisioners(Collections.singletonMap("raw", new RawInputProvisioner()));
    config.setWorkflowEngines(new HashMap<>());
    config.setOutputProvisioners(new HashMap<>());
    config.setRuntimeProvisioners(new HashMap<>());
    config.setTargets(new HashMap<>());
    config.setUnloadDirectory(unloadDirectory.getRoot().getAbsolutePath());
    return config;
  }

  @BeforeClass
  public static void setup() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone("America/Toronto"));
    config = getTestServerConfig(pg);
    main = new Main(config);
    main.startServer(main);
    RestAssured.baseURI = config.getUrl();
    RestAssured.port = config.getPort();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    defaultParser = Parser.TEXT;
    MAPPER.registerModule(new JavaTimeModule());
  }

  @Before
  public void cleanAndMigrateDB() {
    final var simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {config.getDbHost()});
    simpleConnection.setPortNumbers(new int[] {config.getDbPort()});
    simpleConnection.setDatabaseName(config.getDbName());
    simpleConnection.setUser(config.getDbUser());
    simpleConnection.setPassword(config.getDbPass());
    var fw = Flyway.configure().dataSource(simpleConnection);
    fw.load().clean();
    fw.locations("classpath:db/migration").load().migrate();
    // we do this because Flyway on its own isn't finding the test data, and it dies when you
    // try to give it classpath + filesystem locations in one string. We ignore the "missing"
    // migrations (run in the migrate() call above).
    fw.locations("filesystem:src/test/resources/db/migration/")
        .ignoreMissingMigrations(true)
        .load()
        .migrate();
  }

  @AfterClass
  public static void tearDownClass() {
    config = null;
    main = null;
  }
}
