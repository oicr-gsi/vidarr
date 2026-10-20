package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.api.UnloadFilterExternalId;
import ca.on.oicr.gsi.vidarr.api.UnloadFilterWorkflowLabel;
import org.junit.Assert;
import org.junit.Test;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.json.JsonMapper;

public class UnloadFilterTest {
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  @Test
  public void testDeserializeWithProvider() {
    final var filter =
        MAPPER.readValue(
            "{\"type\":\"vidarr-external-id\",\"provider\":\"pinery-miso\",\"id\":\"1_1\"}",
            UnloadFilter.class);
    Assert.assertTrue(filter instanceof UnloadFilterExternalId);
    Assert.assertEquals("pinery-miso", ((UnloadFilterExternalId) filter).getProvider());
  }

  @Test
  public void testDeserializeMissingProviderThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-external-id\",\"id\":\"1_1\"}", UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("provider"));
  }

  @Test
  public void testDeserializeNullProviderThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-external-id\",\"provider\":null,\"id\":\"1_1\"}",
                    UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("provider"));
  }

  @Test
  public void testDeserializeWithLabelAndValue() {
    final var filter =
        MAPPER.readValue(
            "{\"type\":\"vidarr-workflow-label\",\"label\":\"reference\",\"value\":\"hg38\"}",
            UnloadFilter.class);
    Assert.assertTrue(filter instanceof UnloadFilterWorkflowLabel);
    Assert.assertEquals("reference", ((UnloadFilterWorkflowLabel) filter).getLabel());
  }

  @Test
  public void testDeserializeMissingLabelThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-workflow-label\",\"value\":\"hg38\"}", UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("label"));
  }

  @Test
  public void testDeserializeNullLabelThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-workflow-label\",\"label\":null,\"value\":\"hg38\"}",
                    UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("label"));
  }

  @Test
  public void testDeserializeMissingValueThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-workflow-label\",\"label\":\"reference\"}",
                    UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("value"));
  }

  @Test
  public void testDeserializeNullValueThrows() {
    final var e =
        Assert.assertThrows(
            DatabindException.class,
            () ->
                MAPPER.readValue(
                    "{\"type\":\"vidarr-workflow-label\",\"label\":\"reference\",\"value\":null}",
                    UnloadFilter.class));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("value"));
  }
}
