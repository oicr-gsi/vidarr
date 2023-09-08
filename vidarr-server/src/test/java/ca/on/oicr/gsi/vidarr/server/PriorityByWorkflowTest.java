package ca.on.oicr.gsi.vidarr.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse.Visitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class PriorityByWorkflowTest {

  private PriorityByWorkflow sut;
  private ObjectMapper mapper = new ObjectMapper();

  String workflow = "test";
  String version = "1.0";
  ConsumableResourceResponse.Visitor<Optional<String>> consumableResourceCheckerVisitor = new Visitor<Optional<String>>() {
    @Override
    public Optional<String> available() {
      return Optional.empty();
    }

    @Override
    public Optional<String> error(String message) {
      return Optional.of(message);
    }

    @Override
    public Optional<String> unavailable() {
      return Optional.of(String.format("Resource is not available"));
    }
  };

  @Before
  public void instantiate() {
    sut = new PriorityByWorkflow();
  }

  @Test
  public void testRequest_invalidPriorityReturnsError() {
    JsonNode invalidJson = mapper.valueToTree(5);

    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.of(invalidJson)).apply(consumableResourceCheckerVisitor);
    assertTrue(requestError.isPresent());
    assertEquals(requestError.get(), "Vidarr error: The workflow 'test' run's priority (5) is "
        + "invalid. Priority values should be one of the following: 1, 2, 3, 4");
  }

  @Test
  public void testRequest_emptyInputAndEmptyWaitingIsOk() {
    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.empty()).apply(consumableResourceCheckerVisitor);

    assertTrue(requestError.isEmpty());
  }

  @Test
  public void testRequest_validInputAndEmptyWaitingIsOk() {
    JsonNode validJson = mapper.valueToTree(2);
    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.of(validJson)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestError.isEmpty());
  }

  @Test
  public void testIfWorkflowRunWithHigherPriorityExists_thenWorkflowRunDoesNotLaunch() {
    JsonNode higherPriority = mapper.valueToTree(4);
    JsonNode lowerPriority = mapper.valueToTree(2);

    sut.set(workflow, "qwerty", Optional.of(higherPriority));

    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.of(lowerPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestError.isPresent());
    assertEquals(requestError.get(), "There are test workflows currently queued up with higher "
        + "priority.");
  }

  @Test
  public void testIfWorkflowRunWithLowerPriorityExists_thenWorkflowRunLaunches() {
    JsonNode higherPriority = mapper.valueToTree(4);
    JsonNode lowerPriority = mapper.valueToTree(2);

    sut.set(workflow, "qwerty", Optional.of(lowerPriority));

    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.of(higherPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestError.isEmpty());
  }

  @Test
  public void testIfWorkflowRunWithSamePriorityExists_thenWorkflowRunLaunches() {
    JsonNode twoPriority = mapper.valueToTree(2);
    JsonNode twoPriorityAlso = mapper.valueToTree(2);

    sut.set(workflow, "qwerty", Optional.of(twoPriority));

    Optional<String> requestError = sut.request(workflow, version, "abcdef",
        Optional.of(twoPriorityAlso)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestError.isEmpty());
  }

  @Test
  public void testIfWorkflowRunWIthEmptyPriorityIsSet_thenWorkflowRunIsAddedWithLowestPriority() {
    JsonNode lowestPriority = mapper.valueToTree(1);

    Optional<String> requestErrorForEmpty = sut.request(workflow, version, "abcdef",
        Optional.empty()).apply(consumableResourceCheckerVisitor);

    Optional<String> requestErrorForLowest = sut.request(workflow, version, "abcdef",
        Optional.of(lowestPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestErrorForEmpty.isEmpty());
    assertTrue(requestErrorForLowest.isEmpty());

  }

  @Test
  public void testIfResourceIsReleasedButNotLaunched_thenWorkflowRunIsAddedBackToWaiting() {

    JsonNode higherPriority = mapper.valueToTree(4);
    JsonNode lowerPriority = mapper.valueToTree(1);

    Optional<String> requestErrorHigher = sut.request(workflow, version, "qwerty",
        Optional.of(higherPriority)).apply(consumableResourceCheckerVisitor);

    sut.release(workflow, version, "qwerty", Optional.of(higherPriority));

    Optional<String> requestErrorLower = sut.request(workflow, version, "abcdef",
        Optional.of(lowerPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestErrorHigher.isEmpty());
    assertTrue(requestErrorLower.isPresent());
    assertEquals(requestErrorLower.get(), "There are test workflows currently queued up with higher "
        + "priority.");

  }

  @Test
  public void testIfResourceIsReleasedAndLaunched_thenWorkflowRunIsNotAddedBackToWaiting() {

    JsonNode higherPriority = mapper.valueToTree(4);
    JsonNode lowerPriority = mapper.valueToTree(1);

    Optional<String> requestErrorHigher = sut.request(workflow, version, "qwerty",
        Optional.of(higherPriority)).apply(consumableResourceCheckerVisitor);

    sut.release(workflow, version, "qwerty", Optional.empty());

    Optional<String> requestErrorLower = sut.request(workflow, version, "abcdef",
        Optional.of(lowerPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestErrorHigher.isEmpty());
    assertTrue(requestErrorLower.isEmpty());

  }

  @Test
  public void testIfResourcePassesRequest_thenWorkflowRunIsRemovedFromWaiting() {

    JsonNode validJson = mapper.valueToTree(2);

    sut.request(workflow, version, "qwerty",
        Optional.of(validJson)).apply(consumableResourceCheckerVisitor);

    Optional<String> requestErrorLower = sut.request(workflow, version, "abcdef",
        Optional.of(validJson)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestErrorLower.isEmpty());

  }

  @Test
  public void testIfWorkflowLowersPriority_WorkflowIsNotBlockedBySelf() {

    JsonNode higherPriority = mapper.valueToTree(4);
    JsonNode lowerPriority = mapper.valueToTree(2);

    sut.set(workflow, "qwerty", Optional.of(higherPriority));


    Optional<String> requestErrorLower = sut.request(workflow, version, "qwerty",
        Optional.of(lowerPriority)).apply(consumableResourceCheckerVisitor);

    assertTrue(requestErrorLower.isEmpty());

  }


}