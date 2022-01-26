package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.*;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rasklaad.blns.NaughtyStrings;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class VeryBadDataIntegrationTest {
  @ClassRule
  public static JdbcDatabaseContainer pg =
      DatabaseBackedTestConfiguration.getTestDatabaseContainer();

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static ServerConfiguration config;
  private static Main main;
  private static final HttpClient CLIENT =
      HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

  @ClassRule
  public static final TemporaryFolder unloadDirectory =
      DatabaseBackedTestConfiguration.getUnloadDirectory();

  @BeforeClass
  public static void setup() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone("America/Toronto"));
    config = DatabaseBackedTestConfiguration.getTestServerConfig(pg, unloadDirectory, 8998);
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

  public List<String> getNaughtyStringList() {
    List<String> blns = NaughtyStrings.getStrings();

    // These indices correspond with strings here
    // https://github.com/minimaxir/big-list-of-naughty-strings/blob/master/blns.json
    // IntStream.concat() only takes two streams
    List<Integer> indices =
        IntStream.concat(
                IntStream.range(0, 31),
                IntStream.concat(IntStream.range(194, 202), IntStream.range(435, 475)))
            .boxed()
            .collect(Collectors.toList());

    // Filtering original list to only get strings we care about
    return IntStream.range(0, blns.size())
        .filter(indices::contains)
        .mapToObj(blns::get)
        .collect(Collectors.toList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void disableWorkflow() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          delete("/api/workflow/{name}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void getWorkflow() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/workflow/{name}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void addWorkflow() throws JsonProcessingException {
    List<String> filteredBlns = getNaughtyStringList();
    var noParamWorkflow = MAPPER.writeValueAsString(new HashMap<>());

    // Vidarr doesn't actually have any restrictions on the name of a workflow so all of these will
    // succeed
    filteredBlns.forEach(
        (naughtyString) -> {
          given()
              .body(noParamWorkflow)
              .when()
              .post("/api/workflow/{name}", naughtyString)
              .then()
              .assertThat()
              .statusCode(201);

          get("/api/workflow/{name}", naughtyString)
              .then()
              .assertThat()
              .body("labels.keySet()", emptyIterable())
              .body("maxInFlight", equalTo(0))
              .body("isActive", equalTo(true));
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void getWorkflowVersion() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/workflow/{name}/{version}", naughtyString, naughtyString)
              .then()
              .assertThat()
              .statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void addWorkflowVersion() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          var body = MAPPER.createObjectNode();
          var outputs = MAPPER.createObjectNode();
          var parameters = MAPPER.createObjectNode();
          outputs.put("fastqs", "files");
          parameters.put("test", "boolean");
          body.put("language", "NIASSA");
          body.set("outputs", outputs);
          body.set("parameters", parameters);
          body.put("workflow", "#!/bin/sh echo '" + naughtyString + "'");

          given()
              .contentType(ContentType.JSON)
              .body(body)
              .when()
              .post("/api/workflow/{name}/{version}", naughtyString, naughtyString)
              .then()
              .assertThat()
              .statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void getProvisionedFileData() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/file/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void getFinishedWorkflowRunData() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/run/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void getWorkflowRunStatus() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/status/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteActiveWorkflowRun() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          delete("/api/status/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void accessProvisionedUrl() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/url/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }
}
