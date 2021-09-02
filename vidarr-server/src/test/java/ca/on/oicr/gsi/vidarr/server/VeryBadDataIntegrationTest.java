package ca.on.oicr.gsi.vidarr.server;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rasklaad.blns.NaughtyStrings;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class VeryBadDataIntegrationTest extends IntegrationTestBase {

  public List<String> getNaughtyStringList() {
    List<String> blns = NaughtyStrings.getStrings();
    // These indices correspond with strings here
    // https://github.com/minimaxir/big-list-of-naughty-strings/blob/master/blns.json
    List<Integer> indices =
        IntStream.concat(IntStream.range(0, 31), IntStream.range(194, 202))
            .boxed()
            .collect(Collectors.toList());
    // Filtering original list to only get strings we care about
    return IntStream.range(0, blns.size())
        .filter(indices::contains)
        .mapToObj(blns::get)
        .collect(Collectors.toList());
  }

  @Test
  public void getWorkflow() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/workflow/{name}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test
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

  @Test
  // todo: this results in an internal server error
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

  @Test
  public void addWorkflowVersion() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          var body = MAPPER.createObjectNode();
          var outputs = MAPPER.createObjectNode();
          var parameters = MAPPER.createObjectNode();
          outputs.put("fastqs", "files");
          parameters.put("test", "boolean");
          body.put("language", "UNIX_SHELL");
          body.set("outputs", outputs);
          body.set("parameters", parameters);
          body.put("workflow", "#!/bin/sh echo '" + naughtyString + "'");

          given()
              .body(body)
              .when()
              .post("/api/workflow/{name}/{version}", naughtyString, naughtyString)
              .then()
              .assertThat()
              .statusCode(404);
        });
  }

  @Test
  public void getProvisionedFileData() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/file/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test
  public void getFinishedWorkflowRunData() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/run/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test
  public void getWorkflowRunStatus() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/status/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }

  @Test
  public void accessProvisionedUrl() {
    List<String> filteredBlns = getNaughtyStringList();
    filteredBlns.forEach(
        (naughtyString) -> {
          get("/api/url/{hash}", naughtyString).then().assertThat().statusCode(404);
        });
  }
}
