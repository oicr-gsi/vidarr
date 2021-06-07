package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.RestAssured;
import io.restassured.common.mapper.TypeRef;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
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
    when().get("/").then().assertThat().statusCode(200);
  }

  @Test
  public void whenGetWorkflows_thenAvailableWorkflowsAreFound() throws JsonProcessingException {
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
    assertThat(activeWorkflows.get(1).get("language"), equalTo("NIASSA"));
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
        .body(noParamWorkflow)
        .when()
        .post("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .statusCode(201);

    get("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .body(
            "labels.keySet()",
            emptyIterable(),
            "maxInFlight",
            equalTo(0),
            "isActive",
            equalTo(true));
  }

  @Test
  public void whenAddWorkflow_thenWorkflowIsNotAvailable() throws JsonProcessingException {
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .body("size()", is(2));

    var noParamWorkflow = MAPPER.writeValueAsString(new HashMap<>());

    given()
        .body(noParamWorkflow)
        .when()
        .post("/api/workflow/{name}", "novel")
        .then()
        .assertThat()
        .statusCode(201);

    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body("size()", is(2));
  }

  @Test
  public void whenAddDuplicateWorkflow_thenWorkflowIsNotAdded() {
    var workflows =
        given()
            .contentType(ContentType.JSON)
            .when()
            .get("/api/workflows")
            .then()
            .assertThat()
            .body("size()", is(2))
            .and()
            .extract()
            .as(new TypeRef<List<Map<String, Object>>>() {});
    assertThat(
        workflows.stream().filter(wf -> "import_fastq".equals(wf.get("name"))).count(),
        greaterThan(0L));

    given().when().post("/api/workflow/import_fastq").then().statusCode(400);
  }

  @Test
  public void whenAddWorkflowVersion_thenWorkflowIsAvailable() throws JsonProcessingException {
    var wfName = "bcl2fastq";
    var version = "1.new.0";
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body(
            "size()",
            is(2),
            "name",
            everyItem(not(hasItem("bcl2fastq"))),
            "version",
            hasItems("1.0.0.12901362", "1.1.0"));

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

    var versions =
        given()
            .contentType(ContentType.JSON)
            .when()
            .get("/api/workflows")
            .then()
            .assertThat()
            .statusCode(200)
            .and()
            .body("name", hasItems("import_fastq", "bcl2fastq"), "size()", is(15))
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {})
            .stream()
            .filter(wf -> wfName.equals(wf.get("name")))
            .map(wf -> wf.get("version"))
            .collect(Collectors.toSet());
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
    return given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
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
  public void whenIncompleteWorkflowVersionIsAdded_thenWorkflowVersionIsNotAdded() {
    var wfName = "import_fastq";
    var wfVersion = "incompl";
    var wfBefore = getWorkflowVersions(wfName);
    assertThat(wfBefore.stream().filter(v -> wfVersion.equals(v)).count(), equalTo(0L));

    ObjectNode wfv = MAPPER.createObjectNode();
    wfv.put("language", "NIASSA");

    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(400)
        .and()
        .body(containsString("workflow"), containsString("outputs"), containsString("parameters"));

    wfv.put("workflow", "#!/bin/sh 'missing some '");
    given()
        .contentType(ContentType.JSON)
        .body(wfv)
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(400)
        .and()
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
        .and()
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
  public void whenAddDuplicateWorkflowVersion_thenVersionIsNotDuplicated() {
    var wfName = "import_fastq";
    var wfVersion = "2.double";

    var versionsBefore = getWorkflowVersions(wfName);
    assertThat(versionsBefore.stream().filter(v -> wfVersion.equals(v)).count(), equalTo(0L));

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
        .log()
        .ifValidationFails(LogDetail.BODY)
        .assertThat()
        .statusCode(201);

    var versionsAfterFirst = getWorkflowVersions(wfName);
    assertThat(versionsAfterFirst.stream().filter(v -> wfVersion.equals(v)).count(), equalTo(1L));

    // And again:
    given()
        .body(wfv_import_fastq)
        .when()
        .post("/api/workflow/{name}/{version}", wfName, wfVersion)
        .then()
        .assertThat()
        .statusCode(201);

    var versionsAfterSecond = getWorkflowVersions(wfName);
    assertThat(versionsAfterSecond.stream().filter(v -> wfVersion.equals(v)).count(), equalTo(1L));
  }

  @Test
  public void whenDisableUnknownWorkflow_thenAvailableWorkflowsAreUnchanged() {
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .body("size()", is(2));

    given().when().delete("/api/workflow/{name}", "novel").then().assertThat().statusCode(404);

    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body("size()", is(2));
  }

  @Test
  public void whenDisableKnownWorkflow_thenAvailableWorkflowsAreUpdated() {
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .body("size()", is(2));

    given()
        .when()
        .delete("/api/workflow/{name}", "import_fastq")
        .then()
        .assertThat()
        .statusCode(200);

    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflows")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body("size()", is(0));
  }

  @Test
  public void whenDeleteWorkflow_thenWorkflowIsInactivated() {
    var workflow = "import_fastq";
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflow/{name}", workflow)
        .then()
        .assertThat()
        .body("isActive", is(true));

    given().when().delete("/api/workflow/{name}", workflow).then().assertThat().statusCode(200);

    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflow/{name}", workflow)
        .then()
        .assertThat()
        .body("isActive", is(false));
  }

  @Test
  public void whenGetUnknownWorkflow_thenNoWorkflowIsFound() {
    given().when().get("/api/workflow/{name}", "garbage").then().assertThat().statusCode(404);
  }

  @Test
  public void whenGetWorkflow_thenWorkflowIsFound() {
    given()
        .contentType(ContentType.JSON)
        .when()
        .get("/api/workflow/{name}", "bcl2fastq")
        .then()
        .assertThat()
        .body("isActive", is(false));
  }

  @Test
  public void whenGetWaiting_thenNoneAreFoundWaiting() {
    var waitingWorkflows =
        given()
            .when()
            .get("/api/waiting")
            .then()
            .statusCode(200)
            .and()
            .extract()
            .body()
            .as(new TypeRef<List<Map<String, Object>>>() {});
    assertThat(waitingWorkflows.size(), is(1));
    assertThat(waitingWorkflows.get(0).get("workflow"), equalTo("bcl2fastq"));
    assertThat(
        ((List<String>) waitingWorkflows.get(0).get("workflowRuns")),
        hasItem("df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7"));
  }

  @Test
  public void whenGetMaxInFlight_thenGetMaxInFlightData() {
    given()
        .when()
        .get("/api/max-in-flight")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body("$", hasKey("timestamp"), "workflows", hasKey("import_fastq"));
  }

  @Test
  public void whenGetUnknownFile_thenNoFileIsFound() {
    given().when().get("/api/file/{hash}", "abcdefedcbabcdefedcba").then().statusCode(404);
  }

  @Test
  public void whenGetFile_thenFileIsFound() throws InterruptedException {
    var foundFile =
        given()
            .when()
            .contentType(ContentType.JSON)
            .get(
                "/api/file/{hash}",
                "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2")
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
    given()
        .when()
        .contentType(ContentType.JSON)
        .get("/api/file/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetProvenance_thenProvenanceIsReturned() {
    var requestBody = MAPPER.createObjectNode();
    var analysisTypes = MAPPER.createArrayNode();
    analysisTypes.add("FILE");
    requestBody.set("analysisTypes", analysisTypes);
    requestBody.put("epoch", 0);
    requestBody.put("includeParameters", true);
    requestBody.put("timestamp", 0);
    requestBody.put("versionPolicy", "NONE");
    var versionTypes = MAPPER.createArrayNode();
    versionTypes.add("string");
    requestBody.set("versionTypes", versionTypes);

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
            .jsonPath()
            .getList("results");
    assertThat(results, hasSize(10));
  }

  @Test
  public void whenGetProvenanceAfterGivenTimestamp_thenRecordsAfterGivenTimestampAreReturned()
      throws ParseException {
    var endTime = 1577836860000L;
    var requestBody = MAPPER.createObjectNode();
    var analysisTypes = MAPPER.createArrayNode();
    analysisTypes.add("FILE");
    requestBody.set("analysisTypes", analysisTypes);
    requestBody.put("epoch", 0);
    requestBody.put("includeParameters", true);
    requestBody.put("timestamp", endTime); // 2020-01-01 00:01:00
    requestBody.put("versionPolicy", "NONE");
    var versionTypes = MAPPER.createArrayNode();
    versionTypes.add("string");
    requestBody.set("versionTypes", versionTypes);

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
    assertThat(results, hasSize(6));

    for (Map<String, Object> r : results) {
      long modTime = dateFromTime((String) r.get("modified"));
      assertThat(modTime, greaterThan(endTime));
    }
  }

  @Test
  public void whenGetWorkflowRun_thenReturnWorkflowRun() {
    given()
        .when()
        .get("/api/run/{hash}", "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body(
            "workflowName", equalTo("bcl2fastq"), "arguments.workflowRunSWID", equalTo("4444444"));
  }

  @Test
  public void whenGetUnknownWorkflowRun_thenNoWorkflowRunIsFound() {
    given()
        .when()
        .get("/api/run/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetWorkflowStatus_thenWorkflowStatusIsReturned() {
    given()
        .when()
        .get(
            "/api/status/{hash}",
            "df7df7df7df7df7df7df7df7df7df70df7df7df7df7df7df7df7df7df7df7df7")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body(
            "completed",
            nullValue(),
            "operationStatus",
            equalTo("N/A"),
            "waiting_resource",
            equalTo("prometheus-alert-manager"),
            "enginePhase",
            equalTo(Phase.WAITING_FOR_RESOURCES.toString()));
  }

  @Test
  public void whenGetUnknownWorkflowStatus_thenNoWorkflowStatusIsReturned() {
    given()
        .when()
        .get("/api/status/{hash}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .then()
        .assertThat()
        .statusCode(404);
  }

  @Test
  public void whenGetCompletedWorkflowStatus_thenWorkflowStatusIsReturned() {
    given()
        .when()
        .get(
            "/api/status/{hash}",
            "2f52b25df0a20cf41b0476b9114ad40a7d8d2edbddf0bed7d2d1b01d3f2d2b56")
        .then()
        .assertThat()
        .statusCode(200)
        .and()
        .body(
            "completed",
            not(nullValue()),
            "operationStatus",
            equalTo("N/A"),
            "waiting_resource",
            nullValue(),
            "enginePhase",
            nullValue());
  }

  @Test
  public void whenGetWorkflowRunUrlForFileType_thenNoWorkflowRunIsReturned() {
    given()
        .when()
        .get("/api/url/{hash}", "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2")
        .then()
        .assertThat()
        .statusCode(404);
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

  private long dateFromTime(String timeString) throws ParseException {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    df.setTimeZone(TimeZone.getTimeZone("EST"));
    return df.parse(timeString).getTime();
  }
}
