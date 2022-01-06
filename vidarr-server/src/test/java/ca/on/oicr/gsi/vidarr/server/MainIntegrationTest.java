package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.RestAssured;
import io.restassured.common.mapper.TypeRef;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.path.json.JsonPath;
import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static ServerConfiguration config;
  private static Main main;
  private static final HttpClient CLIENT =
      HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();
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

  @Test
  public void whenGetHomepage_then200Response() {
    get("/").then().assertThat().statusCode(200);
  }

  @Test
  public void whenGetWorkflows_thenAvailableWorkflowsAreFound() {
    List<Map<String, Object>> activeWorkflows = get("/api/workflows").as(new TypeRef<>() {});
    assertTrue(activeWorkflows.size() > 1);
    var importFastq1 =
        activeWorkflows.stream()
            .filter(
                w ->
                    w.get("name").equals("import_fastq")
                        && w.get("version").equals("1.0.0.12901362"))
            .findFirst()
            .get();

    assertThat(importFastq1.get("language"), equalTo("NIASSA"));
    assertThat(importFastq1.get("labels"), is(nullValue()));
    assertThat(importFastq1, hasKey("metadata"));
    Map<String, String> metadata = (Map<String, String>) importFastq1.get("metadata");
    assertThat(metadata, hasKey("fastqs"));
    assertThat(metadata.get("fastqs"), equalTo("files"));
    assertThat(importFastq1, hasKey("parameters"));
    Map<String, String> parameters = (Map<String, String>) importFastq1.get("parameters");
    assertThat(parameters, hasKey("workflowRunSWID"));
    assertThat(parameters.get("workflowRunSWID"), equalTo("integer"));

    assertTrue(
        activeWorkflows.stream()
            .anyMatch(
                w ->
                    "import_fastq".equals(w.get("name"))
                        && "1.1.0".equals(w.get("version"))
                        && "NIASSA".equals(w.get("language"))));
  }

  @Test
  public void whenAddWorkflow_thenWorkflowIsAdded() throws JsonProcessingException {
    get("/api/workflow/{name}", "novel").then().assertThat().statusCode(404);

    var noParamWorkflow = MAPPER.writeValueAsString(new HashMap<>());

    given()
        .body(noParamWorkflow)
        .when()
        .post("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .statusCode(201);

    get("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .body("labels.keySet()", emptyIterable())
        .body("maxInFlight", equalTo(0))
        .body("isActive", equalTo(true));
  }

  @Test
  public void whenReAddWorkflow_thenWorkflowMaxInFlightIsUpdated() {
    var bcl2fastq = get("/api/workflow/{name}", "bcl2fastq").then().extract().jsonPath();
    assertNull(bcl2fastq.get("labels"));

    var newValues = MAPPER.createObjectNode();
    var newLabels = newValues.putObject("labels");
    newLabels.put("importantParam", "string");
    newValues.put("maxInFlight", ((Integer) bcl2fastq.get("maxInFlight")) + 5);

    given()
        .body(newValues)
        .when()
        .post("/api/workflow/{name}", "bcl2fastq")
        .then()
        .assertThat()
        .statusCode(201);

    var newBcl2fastq = get("/api/workflow/{name}", "bcl2fastq").then().extract().jsonPath();

    // Labels can't be updated after workflow creation (because they apply to all instances of
    // the workflow, including ones already run), so the ones we tried to add should be ignored.
    assertNull(newBcl2fastq.get("labels"));
    assertNotEquals(newBcl2fastq.get("maxInFlight"), bcl2fastq.get("maxInFlight"));
  }

  @Test
  public void whenAddDuplicateWorkflowParams_thenWorkflowIsUnchanged() {
    var importFastq = get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();
    var originalWorkflowCount =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .size();

    var newValues = MAPPER.createObjectNode();
    newValues.set("labels", importFastq.get("labels"));
    newValues.put("maxInFlight", ((Integer) importFastq.get("maxInFlight")));

    given()
        .body(newValues)
        .when()
        .post("/api/workflow/{name}", "import_fastq")
        .then()
        .assertThat()
        .statusCode(201);

    var newImportFastq = get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();
    var updatedWorkflowCount =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .size();

    assertEquals(importFastq.get("isActive").toString(), newImportFastq.get("isActive").toString());
    assertNull(importFastq.get("labels"));
    assertNull(newImportFastq.get("labels"));
    assertEquals(
        importFastq.get("maxInFlight").toString(), newImportFastq.get("maxInFlight").toString());
    assertEquals(originalWorkflowCount, updatedWorkflowCount);
  }

  @Test
  public void whenUpdateWorkflowFields_thenOnlySomeFieldsAreUpdated()
      throws JsonProcessingException {
    // Only maxInFlight is possible to update from the client side, and it's not possible to set
    // isActive to false.
    var importFastq = get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();

    var newMaxInFlight = 522;
    Map<String, String> newLabels = new HashMap<>();
    newLabels.put("yarn", "string");
    assertTrue(importFastq.get("isActive"));
    var newIsActive = !((Boolean) importFastq.get("isActive")); // note the negate here, we are
    // trying to set it to false

    assertNotEquals(String.valueOf(newMaxInFlight), importFastq.get("maxInFlight"));
    assertNull(importFastq.get("labels"));
    assertNotEquals(newIsActive, importFastq.get("isActive"));

    // maxInFlight should be modifiable
    var modifyMaxInFlight = MAPPER.createObjectNode();
    modifyMaxInFlight.put("maxInFlight", newMaxInFlight);

    given()
        .body(modifyMaxInFlight)
        .when()
        .post("/api/workflow/{name}", "import_fastq")
        .then()
        .assertThat()
        .statusCode(201);
    var mifImportFastq = get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();
    assertTrue(newMaxInFlight == ((Integer) mifImportFastq.get("maxInFlight")));

    // labels should NOT be modifiable
    Map<String, Map<String, String>> modifyLabels = new HashMap<>();
    modifyLabels.put("labels", newLabels);

    given()
        .body(MAPPER.writeValueAsString(modifyLabels))
        .when()
        .post("/api/workflow/{name}", "import_fastq")
        .then()
        .assertThat()
        .statusCode(201);
    var labelsImportFastq = get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();
    assertNull(labelsImportFastq.get("labels"));

    // isActive should NOT be modifiable by the client
    var modifyIsActive = MAPPER.createObjectNode();
    modifyIsActive.put("isActive", newIsActive);

    given()
        .body(MAPPER.writeValueAsString(modifyIsActive))
        .when()
        .post("/api/workflow/{name}", "import_fastq")
        .then()
        .assertThat()
        .statusCode(201);
    var isActiveImportFastq =
        get("/api/workflow/{name}", "import_fastq").then().extract().jsonPath();
    assertNotEquals(newIsActive, isActiveImportFastq.get("isActive"));
  }

  @Test
  public void whenAddDuplicateWorkflowName_thenWorkflowIsNotAdded() {
    var workflows =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .as(new TypeRef<List<Map<String, Object>>>() {});
    assertThat(
        workflows.stream().filter(wf -> "import_fastq".equals(wf.get("name"))).count(),
        greaterThan(0L));

    given().when().post("/api/workflow/{name}", "import_fastq").then().statusCode(400);
  }

  @Test
  public void whenAddWorkflow_thenWorkflowIsNotAvailable() throws JsonProcessingException {
    var oldSize =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .size();

    var noParamWorkflow = MAPPER.writeValueAsString(new HashMap<>());

    given()
        .body(noParamWorkflow)
        .when()
        .post("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .statusCode(201);

    var newSize =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .size();
    assertEquals(oldSize, newSize);
  }

  @Test
  public void whenAddWorkflowVersion_thenWorkflowIsAvailable() {
    var wfName = "bcl2fastq";
    var version = "1.new.0";
    var oldSize =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .body("name", everyItem(not(hasItem("bcl2fastq"))))
            .body("version", hasItems("1.0.0.12901362", "1.1.0"))
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .size();

    var body = MAPPER.createObjectNode();
    body.put("language", "UNIX_SHELL");
    var outputs = MAPPER.createObjectNode();
    outputs.put("fastqs", "files"); // metadata field in db
    body.set("outputs", outputs);
    var parameters = MAPPER.createObjectNode();
    parameters.put("workflowRUnSWID", "integer");
    body.set("parameters", parameters);
    body.put("workflow", "#!/bin/sh echo 'New bcl2fastq dropped'");
    given()
        .body(body)
        .when()
        .post("/api/workflow/{name}/{version}", wfName, version)
        .then()
        .assertThat()
        .statusCode(201);
    // Adding this makes the bcl2fastq workflow and all its versions available

    var updated =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .body("name", hasItems("import_fastq", "bcl2fastq"))
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});
    var newSize = updated.size();
    var versions =
        updated.stream()
            .filter(wf -> wfName.equals(wf.get("name")))
            .map(wf -> wf.get("version"))
            .collect(Collectors.toSet());
    assertTrue(newSize > oldSize);
    assertThat(versions, hasItem(version));
  }

  @Test
  public void whenAddWorkflowVersionToUnknownWorkflow_thenWorkflowVersionIsNotAdded() {
    ObjectNode wfv_import_fastq = MAPPER.createObjectNode();
    ObjectNode parameters = MAPPER.createObjectNode();
    parameters.put("workflowRunSWID", "integer");
    wfv_import_fastq.set("parameters", parameters);
    ObjectNode outputs = MAPPER.createObjectNode();
    outputs.put("fakeqs", "files");
    wfv_import_fastq.set("outputs", outputs);
    wfv_import_fastq.put("language", "NIASSA");
    wfv_import_fastq.put("workflow", "#!/bin/sh echo 'what a mystery'");

    var wfName = "nonexistent";
    var wfVersion = "0.0";

    given()
        .contentType(ContentType.JSON)
        .body(wfv_import_fastq)
        .when()
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(404);
  }

  private Set<Object> getWorkflowVersions(String wfName) {
    return get("/api/workflows")
        .then()
        .assertThat()
        .statusCode(200)
        .body("name", hasItem(wfName))
        .and()
        .assertThat()
        .extract()
        .body()
        .as(new TypeRef<List<Map<String, Object>>>() {})
        .stream()
        .filter(wf -> wfName.equals(wf.get("name")))
        .map(wf -> wf.get("version"))
        .collect(Collectors.toSet());
  }

  @Test
  public void whenGetWorkflowVersion_thenWorkflowVersionIsFound() {
    given()
        .get("/api/workflow/{workflow}/{version}", "fastqc", "1.0.0")
        .then()
        .assertThat()
        .statusCode(200)
        .body(
            containsString("name"),
            containsString("version"),
            containsString("outputs"),
            containsString("language"),
            not(containsString("workflow")));
  }

  @Test
  public void whenGetWorkflowVersionWithDefinition_thenWorkflowVersionWithDefinitionIsFound() {
    given()
        .get("/api/workflow/{workflow}/{version}?includeDefinitions=true", "fastqc", "1.0.0")
        .then()
        .assertThat()
        .statusCode(200)
        .body(
            containsString("name"),
            containsString("version"),
            containsString("outputs"),
            containsString("language"),
            containsString("workflow"),
            containsString("accessoryFiles"));
  }

  @Test
  public void whenAddExistingWorkflowVersion_thenAddIsRejected() throws JsonProcessingException {
    var workflowName = "fastqc";
    var workflowVersion = "1.0.0";
    var existingFastqc =
        get(
                "/api/workflow/{workflow}/{version}?includeDefinitions=true",
                workflowName,
                workflowVersion)
            .then()
            .extract()
            .body()
            .as(new TypeRef<Map<String, Object>>() {});
    given()
        .contentType(ContentType.JSON)
        .body(MAPPER.writeValueAsString(existingFastqc))
        .when()
        .post("/api/workflow/{workflow}/{version}", workflowName, workflowVersion)
        .then()
        .assertThat()
        .statusCode(409);
  }

  @Test
  public void whenIncompleteWorkflowVersionIsAdded_thenWorkflowVersionIsNotAdded() {
    var wfName = "import_fastq";
    var wfVersion = "incompl";
    var wfBefore = getWorkflowVersions(wfName);
    assertThat(wfBefore.stream().filter(wfVersion::equals).count(), equalTo(0L));

    ObjectNode wfv = MAPPER.createObjectNode();
    wfv.put("language", "NIASSA");

    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(400)
        .body(containsString("workflow"), containsString("outputs"), containsString("parameters"));

    wfv.put("workflow", "#!/bin/sh 'missing some '");
    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(400)
        .body(
            not(containsString("workflow")),
            containsString("outputs"),
            containsString("parameters"));

    ObjectNode outputs = MAPPER.createObjectNode();
    outputs.put("fas", "files");
    wfv.set("outputs", outputs);
    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(400)
        .body(
            not(containsString("workflow")),
            not(containsString("outputs")),
            containsString("parameters"));

    ObjectNode parameters = MAPPER.createObjectNode();
    parameters.put("workflowRunSWID", "integer");
    wfv.set("parameters", parameters);

    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(201);
  }

  @Test
  public void whenAddDuplicateWorkflowVersion_thenWorkflowVersionIsUnchanged() {
    var wfName = "import_fastq";
    var wfVersion = "2.double";

    var versionsBefore = getWorkflowVersions(wfName);
    assertThat(versionsBefore.stream().filter(wfVersion::equals).count(), equalTo(0L));

    ObjectNode wfv_import_fastq = MAPPER.createObjectNode();
    ObjectNode parameters = MAPPER.createObjectNode();
    parameters.put("workflowRunSWID", "integer");
    wfv_import_fastq.set("parameters", parameters);
    ObjectNode outputs = MAPPER.createObjectNode();
    outputs.put("fastqs", "files");
    wfv_import_fastq.set("outputs", outputs);
    wfv_import_fastq.put("language", "NIASSA");
    wfv_import_fastq.put("workflow", "#!/bin/sh echo 'double me up'");

    given()
        .contentType(ContentType.JSON)
        .body(wfv_import_fastq)
        .when()
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(201);

    var versionsAfterFirst = getWorkflowVersions(wfName);
    assertThat(versionsAfterFirst.stream().filter(wfVersion::equals).count(), equalTo(1L));

    // And again:
    given()
        .body(wfv_import_fastq)
        .when()
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(409);

    var versionsAfterSecond = getWorkflowVersions(wfName);
    assertThat(versionsAfterSecond.stream().filter(wfVersion::equals).count(), equalTo(1L));
  }

  @Test
  public void whenDisableUnknownWorkflow_thenAvailableWorkflowsAreUnchanged() {
    var before =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});

    delete("/api/workflow/{name}", "novel").then().assertThat().statusCode(404);

    var after =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});

    assertEquals(before.size(), after.size());
  }

  @Test
  public void whenDisableKnownWorkflow_thenAvailableWorkflowsAreUpdated() {
    var before =
        get("/api/workflows")
            .then()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});

    delete("/api/workflow/{name}", "import_fastq").then().assertThat().statusCode(200);

    var after =
        get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});

    assertTrue(before.size() > after.size());
  }

  @Test
  public void whenDeleteWorkflow_thenWorkflowIsInactivated() {
    var workflow = "import_fastq";
    get("/api/workflow/{name}", workflow)
        .then()
        .assertThat()
        .statusCode(200)
        .body("isActive", is(true));

    delete("/api/workflow/{name}", workflow).then().assertThat().statusCode(200);

    get("/api/workflow/{name}", workflow)
        .then()
        .assertThat()
        .statusCode(200)
        .body("isActive", is(false));
  }

  @Test
  public void whenGetUnknownWorkflow_thenNoWorkflowIsFound() {
    get("/api/workflow/{name}", "garbage").then().assertThat().statusCode(404);
  }

  @Test
  public void whenGetWorkflow_thenWorkflowIsFound() {
    get("/api/workflow/{name}", "bcl2fastq")
        .then()
        .assertThat()
        .statusCode(200)
        .body("isActive", is(false));
  }

  @Test
  public void whenGetWaiting_thenWaitingWorkflowsAreFound() {
    var waitingBcl2fastq =
        get("/api/waiting")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .stream()
            .filter(w -> "bcl2fastq".equals(w.get("workflow")))
            .findFirst();
    assertNotNull(waitingBcl2fastq);
    assertEquals(waitingBcl2fastq.get().get("oldest"), "2015-02-19T03:00:48.262-05:00");
    assertThat(
        ((List<String>) waitingBcl2fastq.get().get("workflowRuns")),
        hasItem("df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7"));
  }

  @Test
  public void whenGetMaxInFlight_thenGetMaxInFlightData() {
    get("/api/max-in-flight")
        .then()
        .assertThat()
        .statusCode(200)
        .body("$", hasKey("timestamp"), "workflows", hasKey("import_fastq"));
  }

  @Test
  public void whenGetUnknownFile_thenNoFileIsFound() {
    get("/api/file/{hash}", "abcdefedcbabcdefedcba").then().statusCode(404);
  }

  @Test
  public void whenGetFile_thenFileIsFound() {
    var foundFile =
        get("/api/file/{hash}", "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(ObjectNode.class);

    var given = getAnalysisFile();
    assertEquals(given, foundFile);
  }

  @Test
  public void whenUnknownFileIsRequested_thenNoFileIsReturned() {
    get("/api/file/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  private ObjectNode buildProvenanceRequestBody(
      String versionPolicy, Instant epoch, Instant timestamp) {
    var requestBody = MAPPER.createObjectNode();
    var analysisTypes = requestBody.putArray("analysisTypes");
    analysisTypes.add("FILE");
    requestBody.put("epoch", (epoch == null ? 0 : epoch.toEpochMilli()));
    requestBody.put("includeParameters", true);
    requestBody.put("timestamp", timestamp.toEpochMilli());
    requestBody.put("versionPolicy", versionPolicy);
    var versionTypes = requestBody.putArray("versionTypes");
    versionTypes.add("pinery-hash-1");
    versionTypes.add("pinery-hash-2");
    versionTypes.add("pinery-hash-7");
    versionTypes.add("pinery-hash-8");
    return requestBody;
  }

  @Test
  public void whenGetProvenanceRecordsLatestVersion_thenLatestVersionIsReturned() {
    var requestBody =
        buildProvenanceRequestBody("LATEST", Instant.ofEpochMilli(0), Instant.ofEpochMilli(0));
    // we really provenance to pick this targetVersion and not the f8f8f8f8 one which was the
    // same provider version but created earlier
    String targetVersion = "f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9";
    String antiTargetVersion = "f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8";

    List<ObjectNode> response =
        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
            .when()
            .post("/api/provenance")
            .then()
            .extract()
            .body()
            .as(ProvenanceResponse.class)
            .getResults();
    var externalKeysList =
        response.stream().map(r -> r.get("externalKeys")).collect(Collectors.toSet());

    // Do this in two steps because JsonNode doesn't want to stream
    var nodeVersions = new ArrayList<>(); // [{}]
    externalKeysList.forEach(
        ekl ->
            ekl.forEach(
                ek -> {
                  nodeVersions.add(ek.get("versions"));
                }));

    assertTrue(
        nodeVersions.stream()
            .map(v -> MAPPER.convertValue(v, new TypeReference<Map<String, String>>() {}))
            .map(v -> v.values())
            .anyMatch(a -> a.contains(targetVersion) && !a.contains(antiTargetVersion)));
  }

  @Test
  public void whenGetProvenanceRecordsNoneVersion_thenNullVersionsAreReturned() {
    var requestBody =
        buildProvenanceRequestBody("NONE", Instant.ofEpochMilli(0), Instant.ofEpochMilli(0));

    var results =
        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
            .when()
            .post("/api/provenance")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(ProvenanceResponse.class)
            .getResults();
    var externalKeysList =
        results.stream().map(r -> r.get("externalKeys")).collect(Collectors.toSet());

    externalKeysList.forEach(
        ekl ->
            ekl.forEach(
                ek -> {
                  assertTrue(ek.get("versions") == null || ek.get("versions").isNull());
                }));
  }

  @Test
  public void whenGetProvenanceRecordsAllVersions_thenAllVersionsAreReturned() {
    var requestBody =
        buildProvenanceRequestBody("ALL", Instant.ofEpochMilli(0), Instant.ofEpochMilli(0));
    Set<String> targetVersions = new HashSet<>();
    targetVersions.add("f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2");
    targetVersions.add("a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2");
    targetVersions.add("f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8");
    targetVersions.add("f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7");
    targetVersions.add("f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9");

    var response =
        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
            .when()
            .post("/api/provenance")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(ProvenanceResponse.class)
            .getResults();
    var externalKeysList =
        response.stream().map(r -> r.get("externalKeys")).collect(Collectors.toSet());

    // Do this as a second step because JsonNode doesn't want to stream
    var versions = new ArrayList<>(); // [{}]
    externalKeysList.forEach(
        ekl ->
            ekl.forEach(
                ek -> {
                  versions.add(ek.get("versions"));
                }));

    // Lucky us, this one gives us a list of all the values for each provider version
    // e.g. [{"pinery-hash-2":["bead860","daef391"]}, {"pinery-hash-1": ["abcd1234"],
    // "pinery-hash-2": ["deff1940"]}]
    var values =
        versions.stream()
            .map(v -> MAPPER.convertValue(v, new TypeReference<Map<String, List<String>>>() {}))
            .map(v -> v.values().stream().flatMap(List::stream).collect(Collectors.toSet()))
            .collect(Collectors.toSet());
    assertTrue(values.stream().anyMatch(v -> v.containsAll(targetVersions)));
  }

  @Test
  public void whenGetProvenanceAfterGivenTimestamp_thenRecordsAfterGivenTimestampAreReturned()
      throws ParseException {
    var requestAllRecords =
        buildProvenanceRequestBody("NONE", Instant.ofEpochMilli(0), Instant.ofEpochMilli(0));

    var allRecordsSize =
        given()
            .contentType(ContentType.JSON)
            .body(requestAllRecords)
            .when()
            .post("/api/provenance")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(ProvenanceResponse.class)
            .getResults()
            .size();

    var endTime = Instant.ofEpochMilli(1577836860000L); // 2020-01-01 00:01:00
    var requestBody = buildProvenanceRequestBody("NONE", Instant.ofEpochMilli(0L), endTime);

    // First request gets us the epoch, which we'll need to get the server to pay attention our
    // timestamp field in the second request
    long epoch =
        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
            .when()
            .post("/api/provenance")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .jsonPath()
            .getLong("epoch");
    requestBody.put("epoch", epoch);

    List<Map<String, Object>> results =
        given()
            .contentType(ContentType.JSON)
            .body(requestBody)
            .when()
            .post("/api/provenance")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .jsonPath()
            .getList("results");
    assertTrue(results.size() < allRecordsSize);

    for (Map<String, Object> r : results) {
      Instant modTime = dateFromTime((String) r.get("modified"));
      assertTrue(modTime.isAfter(endTime));
    }
  }

  @Test
  public void whenGetWorkflowRun_thenReturnWorkflowRun() {
    get("/api/run/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200)
        .body(
            "workflowName", equalTo("bcl2fastq"), "arguments.workflowRunSWID", equalTo("4444444"));
  }

  @Test
  public void whenGetUnknownWorkflowRun_thenNoWorkflowRunIsFound() {
    get("/api/run/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenDeleteActiveWorkflowRun_thenWorkflowRunIsDeleted() {
    get("/api/status/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200);

    delete("/api/status/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200);

    get("/api/status/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenDeleteUnknownWorkflowRun_thenNoWorkflowRunIsDeleted() {
    delete("/api/status/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenDeleteCompletedWorkflowRun_thenNoWorkflowRunIsDeleted() {
    delete("/api/status/{hash}", "5d93d47a8dfc7e038bdc3ffc4b7faf6a53a22c51ae9df7d683aef912510b0a88")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetAllWorkflowStatuses_thenStatusesForAllWorkflowsAreReturned() {
    get("/api/status")
        .then()
        .assertThat()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].completed", nullValue())
        .body("[0].operationStatus", equalTo("N/A"))
        .body("[0].waiting_resource", equalTo("prometheus-alert-manager"))
        .body("[0].enginePhase", equalTo(Phase.WAITING_FOR_RESOURCES.toString()));
  }

  @Test
  public void whenGetWorkflowStatus_thenStatusForWorkflowIsReturned() {
    get("/api/status/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200)
        .body("completed", nullValue())
        .body("operationStatus", equalTo("N/A"))
        .body("waiting_resource", equalTo("prometheus-alert-manager"))
        .body("enginePhase", equalTo(Phase.WAITING_FOR_RESOURCES.toString()));
  }

  @Test
  public void whenGetUnknownWorkflowStatus_thenNoWorkflowStatusIsReturned() {
    get("/api/status/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetCompletedWorkflowStatus_thenWorkflowStatusIsReturned() {
    get("/api/status/{hash}", "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56")
        .then()
        .assertThat()
        .statusCode(200)
        .body("completed", not(nullValue()))
        .body("operationStatus", equalTo("N/A"))
        .body("waiting_resource", nullValue())
        .body("enginePhase", nullValue());
  }

  @Test
  public void whenGetWorkflowRunUrlForFileType_thenNoWorkflowRunIsReturned() {
    get("/api/url/{hash}", "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetWorkflowRunUrl_thenWorkflowRunIsReturned() {
    get("/api/url/{hash}", "8b16674e6e2a36d1f689632b1f36d0fe0876b7d54583dfbdf76c4c58e0588531")
        .then()
        .assertThat()
        .statusCode(200)
        .body("run", equalTo("a5f036ac00769744f9349775b376bf9412a5b28191fb7dd5ca4e635338e9f2b5"))
        .body("labels.keySet()", hasItems("read_count", "read_number", "niassa-file-accession"));
  }

  @Test
  public void whenCopyOut_thenRecordsAreCopied() {
    ObjectNode copyOutFilter = getBcl2FastqUnloadFilter();

    var resp =
        given()
            .contentType(ContentType.JSON)
            .body(copyOutFilter)
            .when()
            .post("/api/copy-out")
            .then()
            .assertThat()
            .statusCode(200)
            .body("workflowRuns.size()", is(8))
            .body("workflowRuns.findAll { it.workflowName == \"bcl2fastq\" }.size()", is(8))
            .and()
            .extract()
            .jsonPath();
    var firstHash = resp.get("workflowRuns[0].id");

    // Confirm run hasn't been unloaded
    get("/api/run/{hash}", firstHash).then().assertThat().statusCode(200);
  }

  @Test
  public void whenUnloadWorkflowRuns_thenFilesAreGone() throws IOException {
    // Confirm that a bcl2fastq workflow run exists
    get("/api/run/{hash}", "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56")
        .then()
        .assertThat()
        .statusCode(200)
        .body("workflowName", equalTo("bcl2fastq"));

    ObjectNode unloadFilter = getBcl2FastqUnloadFilter();

    var unloadFileQuotedName =
        given()
            .contentType(ContentType.JSON)
            .body(unloadFilter)
            .when()
            .post("/api/unload")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .asInputStream();

    var unloadFileName = MAPPER.readTree(unloadFileQuotedName).toString().replaceAll("\"", "");
    var unloadedFilePath = unloadDirectory.getRoot().getAbsolutePath() + "/" + unloadFileName;

    var unloaded = JsonPath.from(new File(unloadedFilePath));
    assertThat(unloaded.getList("workflowRuns").size(), equalTo(8));
    assertThat(
        unloaded.getList("workflowRuns.findAll { it.workflowName == \"bcl2fastq\" }").size(),
        equalTo(8));
    var firstHash = unloaded.get("workflowRuns[0].id");

    // Confirm that the bcl2fastq workflow run has been unloaded from the database
    get("/api/run/{hash}", firstHash).then().assertThat().statusCode(404);
  }

  @Test
  public void whenWorkflowRunsAreUnloaded_thenTheyCanBeLoaded() throws IOException {
    var bcl2fastqHash = "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56";
    // Confirm that the bcl2fastq workflow run exists in the database
    get("/api/run/{hash}", bcl2fastqHash).then().assertThat().statusCode(200);

    ObjectNode unloadFilter = getBcl2FastqUnloadFilter();

    var unloadFileQuotedName =
        given()
            .contentType(ContentType.JSON)
            .body(unloadFilter)
            .when()
            .post("/api/unload")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .asInputStream();

    var unloadFileName = MAPPER.readTree(unloadFileQuotedName).toString().replaceAll("\"", "");
    var unloadedFilePath = unloadDirectory.getRoot().getAbsolutePath() + "/" + unloadFileName;

    var unloaded = MAPPER.readTree(new File(unloadedFilePath));

    // Confirm that the bcl2fastq workflow run has been unloaded from the database
    get("/api/run/{hash}", bcl2fastqHash).then().assertThat().statusCode(404);

    given()
        .contentType(ContentType.JSON)
        .body(unloaded)
        .when()
        .post("/api/load")
        .then()
        .assertThat()
        .statusCode(200);

    // Confirm that the bcl2fastq workflow run has been loaded back into the database
    get("/api/run/{hash}", bcl2fastqHash).then().assertThat().statusCode(200);

    // Confirm that unloading the same data again produces the same result
    var unload2FileQuotedName =
        given()
            .contentType(ContentType.JSON)
            .body(unloadFilter)
            .when()
            .post("/api/unload")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .extract()
            .asInputStream();
    var unload2FileName = MAPPER.readTree(unload2FileQuotedName).toString().replaceAll("\"", "");
    var unloaded2FilePath = unloadDirectory.getRoot().getAbsolutePath() + "/" + unload2FileName;

    var reUnloaded = MAPPER.readTree(new File(unloaded2FilePath));
    // Created and modified fields will be affected by our re-loading them into the db
    removeCreatedAndModifiedFieldsForBetterComparisons(unloaded);
    removeCreatedAndModifiedFieldsForBetterComparisons(reUnloaded);
    assertEquals(reUnloaded, unloaded);
  }

  @Test
  public void whenExternalIdsAreUpdated_thenUpdatedVersionsAreSaved() {
    var runHash = "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56";
    JsonPath initial = get("/api/run/{hash}", runHash).then().extract().jsonPath();
    assertThat(initial.getList("externalKeys"), hasSize(1));
    assertThat(initial.get("externalKeys[0].versions.keySet()"), hasSize(1));

    var bulkUpdate = MAPPER.createObjectNode();
    bulkUpdate.put("newVersionKey", "pinery-hash-52");
    bulkUpdate.put("oldVersionKey", "pinery-hash-2");
    bulkUpdate.put("provider", "pinery-miso");
    var updates = bulkUpdate.putArray("updates");
    var first = MAPPER.createObjectNode();
    first.put("add", "fadefadefadefadefadefadefadefadefadefadefadefadefadefadefadefade");
    first.put("old", "bea8063d6c8e66e4c6faae52ddc8e5e7ab249782cb98ec7fb64261f12e82a3bf");
    first.put("id", "3786_1_LDI31800");
    updates.add(first);

    given()
        .body(bulkUpdate)
        .post("/api/versions")
        .then()
        .assertThat()
        .statusCode(200)
        .body(equalTo("1"));

    JsonPath updated = get("/api/run/{hash}", runHash).then().extract().jsonPath();
    assertThat(updated.getList("externalKeys"), hasSize(1));
    assertThat(updated.get("externalKeys[0].versions.keySet()"), hasSize(2));

    assertEquals(updated.getString("externalKeys[0].id"), initial.getString("externalKeys[0].id"));
    assertEquals(
        updated.getString("externalKeys[0].provider"),
        initial.getString("externalKeys[0].provider"));
    assertNotEquals(
        updated.getString("externalKeys[0].versions"),
        initial.getString("externalKeys[0].versions"));
    assertThat(
        updated.get("externalKeys[0].versions.keySet()"),
        hasItems("pinery-hash-52", "pinery-hash-2"));
  }

  @Test
  public void whenExternalVersionFieldsMismatch_thenVersionsAreNotUpdated() {
    // mismatch on external_id_version.key (old)
    var bulkUpdateOldKey = MAPPER.createObjectNode();
    bulkUpdateOldKey.put("newVersionKey", "pinery-hash-52");
    bulkUpdateOldKey.put("oldVersionKey", "pinery-hash-0");
    bulkUpdateOldKey.put("provider", "pinery-miso");
    var updatesOldKey = bulkUpdateOldKey.putArray("updates");
    var firstOldKey = MAPPER.createObjectNode();
    firstOldKey.put("add", "fadefadefadefadefadefadefadefadefadefadefadefadefadefadefadefade");
    firstOldKey.put("old", "bea8063d6c8e66e4c6faae52ddc8e5e7ab249782cb98ec7fb64261f12e82a3bf");
    firstOldKey.put("id", "3786_1_LDI31800");
    updatesOldKey.add(firstOldKey);

    given()
        .body(bulkUpdateOldKey)
        .post("/api/versions")
        .then()
        .assertThat()
        .statusCode(200)
        .body(equalTo("0")); // 0 records were updated

    // mismatch on external_id.external_id
    var bulkUpdateExternalId = MAPPER.createObjectNode();
    bulkUpdateExternalId.put("newVersionKey", "pinery-hash-52");
    bulkUpdateExternalId.put("oldVersionKey", "pinery-hash-2");
    bulkUpdateExternalId.put("provider", "pinery-miso");
    var updatesExternalId = bulkUpdateExternalId.putArray("updates");
    var firstExternalId = MAPPER.createObjectNode();
    firstExternalId.put("add", "fadefadefadefadefadefadefadefadefadefadefadefadefadefadefadefade");
    firstExternalId.put("old", "bea8063d6c8e66e4c6faae52ddc8e5e7ab249782cb98ec7fb64261f12e82a3bf");
    firstExternalId.put("id", "1000_1_LDI00001");
    updatesExternalId.add(firstExternalId);

    given()
        .body(bulkUpdateExternalId)
        .post("/api/versions")
        .then()
        .assertThat()
        .statusCode(200)
        .body(equalTo("0")); // 0 records were updated
  }

  private ObjectNode getAnalysisFile() {
    ObjectNode on = MAPPER.createObjectNode();
    on.put("run", "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56");
    on.put("id", "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2");
    on.put("type", "file");
    on.put("created", "2019-07-15T15:27:27.206-04:00");
    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("read_number", "2");
    labels.put("niassa-file-accession", "14718426");
    on.set("labels", labels);
    on.put("modified", "2021-05-14T09:53:52-04:00");
    ObjectNode extKey = MAPPER.createObjectNode();
    extKey.put("id", "3786_1_LDI31800");
    extKey.put("provider", "pinery-miso");
    extKey.put("created", "2021-05-14T09:53:52-04:00");
    extKey.put("modified", "2021-05-14T09:53:52-04:00");
    extKey.put("requested", false);
    ObjectNode versions = MAPPER.createObjectNode();
    ArrayNode version = MAPPER.createArrayNode();
    version.add("bea8063d6c8e66e4c6faae52ddc8e5e7ab249782cb98ec7fb64261f12e82a3bf");
    versions.set("pinery-hash-2", version);
    extKey.set("versions", versions);
    ArrayNode key = MAPPER.createArrayNode();
    key.add(extKey);
    on.set("externalKeys", key);
    on.put(
        "path",
        "/analysis/archive/seqware/seqware_analysis_12/hsqwprod/seqware-results"
            + "/CASAVA_2.9.1/83779816/SWID_14718190_DCRT_016_Br_R_PE_234_MR_obs528_P016_190711_M00146_0072_000000000-D6D3B_ACTGAT_L001_R2_001.fastq.gz");
    on.put("md5", "f48142a9bee7e789c15c21bd34e9adec");
    on.put("metatype", "chemical/seq-na-fastq-gzip");
    on.put("size", 7135629);
    return on;
  }

  private ObjectNode getBcl2FastqUnloadFilter() {
    ObjectNode unloadFilter = MAPPER.createObjectNode();
    unloadFilter.put("recursive", true);
    ObjectNode filterType = MAPPER.createObjectNode();
    filterType.put("type", "vidarr-workflow-name");
    filterType.put("name", "bcl2fastq");
    unloadFilter.set("filter", filterType);
    return unloadFilter;
  }

  private Instant dateFromTime(String timeString) throws ParseException {
    return OffsetDateTime.parse(timeString).toInstant();
  }

  private void removeCreatedAndModifiedFieldsForBetterComparisons(JsonNode unload) {
    unload
        .get("workflowRuns")
        .forEach(
            wfr -> {
              ((ObjectNode) wfr).remove("modified");
              wfr.get("externalKeys")
                  .forEach(
                      ek -> {
                        ((ObjectNode) ek).remove("created");
                        ((ObjectNode) ek).remove("modified");
                      });
              wfr.get("analysis").forEach(a -> ((ObjectNode) a).remove("modified"));
            });
  }

  private static class ProvenanceResponse {
    public ProvenanceResponse() {}

    long epoch;
    List<ObjectNode> results;
    long timestamp;

    public long getEpoch() {
      return epoch;
    }

    public List<ObjectNode> getResults() {
      return results;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setEpoch(long epoch) {
      this.epoch = epoch;
    }

    public void setResults(List<ObjectNode> results) {
      this.results = results;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
  }
}
