package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.common.mapper.TypeRef;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class MainIntegrationTest {
  @ClassRule
  public static JdbcDatabaseContainer pg =
      new PostgreSQLContainer("postgres:12-alpine")
          .withDatabaseName("vidarr-test")
          .withUsername("vidarr-test")
          .withPassword("vidarr-test");

  private final ObjectMapper MAPPER = new ObjectMapper();
  private static ServerConfiguration config;
  private static Main main;
  private static HttpClient CLIENT =
      HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();

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
    return config;
  }

  @BeforeClass
  public static void setup() throws SQLException {
    config = getTestServerConfig(pg);
    main = new Main(config, "TEST");
    main.startServer(main);
    RestAssured.baseURI = config.getUrl();
    RestAssured.port = config.getPort();
    defaultParser = Parser.TEXT;
  }

  @Before
  public void migrate() {
    final var simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {config.getDbHost()});
    simpleConnection.setPortNumbers(new int[] {config.getDbPort()});
    simpleConnection.setDatabaseName(config.getDbName());
    simpleConnection.setUser(config.getDbUser());
    simpleConnection.setPassword(config.getDbPass());
    var fw = Flyway.configure().dataSource(simpleConnection);
    fw.load().clean();
    fw.locations("classpath:db/migration").load().migrate();
    var flywayTestLocations = "filesystem:src/test/resources/db/migration/";
    fw.locations(flywayTestLocations).ignoreMissingMigrations(true).load().migrate();
  }

  @Test
  public void whenGetHomepage_then200Response() {
    when().get("/").then().assertThat().statusCode(200);
  }

  @Test
  public void whenGetWorkflows_thenWorkflowsAreAvailable() throws JsonProcessingException {
    List<Map<String, Object>> activeWorkflows =
        get("/api/workflows").as(new TypeRef<List<Map<String, Object>>>() {});
    assertThat(activeWorkflows, hasSize(2));

    assertThat(activeWorkflows.get(0).get("name"), equalTo("import_fastq"));
    assertThat(activeWorkflows.get(0).get("version"), equalTo("1.0.0.12901362"));
    assertThat(activeWorkflows.get(0).get("language"), equalTo("NIASSA"));
    assertThat(activeWorkflows.get(0).get("labels"), is(nullValue()));
    assertThat(activeWorkflows.get(0), hasKey("metadata"));
    Map<String, String> metadata = (Map<String, String>) activeWorkflows.get(0).get("metadata");
    assertThat(metadata, hasKey("fastqs"));
    assertThat(metadata.get("fastqs"), equalTo("files"));
    assertThat(activeWorkflows.get(0), hasKey("parameters"));
    Map<String, String> parameters = (Map<String, String>) activeWorkflows.get(0).get("parameters");
    assertThat(parameters, hasKey("workflowRunSWID"));
    assertThat(parameters.get("workflowRunSWID"), equalTo("integer"));

    assertThat(activeWorkflows.get(1).get("name"), equalTo("import_fastq"));
    assertThat(activeWorkflows.get(1).get("version"), equalTo("1.1.0"));
    assertThat(activeWorkflows.get(1).get("language"), equalTo("WDL_1_1"));
  }

  @Test
  public void whenAddWorkflow_thenWorkflowIsAdded() throws JsonProcessingException {
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .statusCode(404);

    var noParamWorkflow = MAPPER.writeValueAsString(new HashMap<>());

    given()
        .pathParam("name", "novel")
        .body(noParamWorkflow)
        .when()
        .post("/api/workflow/{name}")
        .then()
        .assertThat()
        .statusCode(200);

    var newWorkflow =
        get("/api/workflow/{name}", "novel").as(new TypeRef<Map<String, Object>>() {});
    assertThat(newWorkflow, equalTo("{}"));
  }

  //  @Test
  //  public void whenAddWorkflowVersion_thenWorkflowVersionIsAdded() {
  //
  //  }
  //
  //  @Test
  //  public void whenaddWorkflowVersion_thenAvailableWorkflowAreUpdated() {
  //
  //  }
}
