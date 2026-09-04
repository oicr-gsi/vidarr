package ca.on.oicr.gsi.vidarr.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Where a failure to resolve an internal ID surfaces
 *
 * <p>The extractor throws when it cannot make sense of an input file, but the streams it builds are
 * lazy, so where that throw lands depends on where the file sits in the parameter. A file that is
 * the whole parameter is visited while {@link InputType#apply} is still running and throws from
 * there. A file nested in a list — or an object, dictionary, or tuple — is not visited until the
 * returned stream is consumed, so the throw lands on the caller's terminal operation instead.
 *
 * <p>That distinction is the whole reason a caller cannot guard this by wrapping the call to {@code
 * apply}: for the nested case, which is the common one, nothing has happened yet when {@code apply}
 * returns.
 */
public class ExtractInputExternalIdsTest {

  /** A resolver that answers the same way every time and counts how often it was asked. */
  private static final class CountingResolver implements FileResolver {

    private final Optional<FileMetadata> answer;
    private int calls;

    CountingResolver(Optional<FileMetadata> answer) {
      this.answer = answer;
    }

    int calls() {
      return calls;
    }

    @Override
    public Optional<FileMetadata> pathForId(String id) {
      calls++;
      return answer;
    }
  }

  private static final String FILE_ID =
      "vidarr:test/file/916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2";
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final String OTHER_FILE_ID =
      "vidarr:test/file/767d00090277cb760d69352c944a30d252e7950a0e89c6ea1951121e8443389f";

  @Test
  public void whenFileIsInsideAList_thenTheFailureSurfacesWhenTheStreamIsConsumed() {
    final CountingResolver resolver = new CountingResolver(Optional.empty());
    final Stream<? extends ExternalId> ids =
        InputType.FILE
            .asList()
            .apply(new ExtractInputExternalIds(MAPPER, listOf(internalFile(FILE_ID)), resolver));

    // apply() has only assembled the stream; the file has not been looked at yet.
    assertEquals(0, resolver.calls());

    assertThrows(IllegalArgumentException.class, ids::toList);
    assertEquals(1, resolver.calls());
  }

  @Test
  public void whenFileIsTheWholeParameter_thenTheFailureSurfacesFromApply() {
    final CountingResolver resolver = new CountingResolver(Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            InputType.FILE.apply(
                new ExtractInputExternalIds(MAPPER, internalFile(FILE_ID), resolver)));
    assertEquals(1, resolver.calls());
  }

  @Test
  public void whenInternalContentsAreMalformed_thenItThrowsWithoutConsultingTheResolver() {
    final CountingResolver resolver = new CountingResolver(Optional.empty());
    final ObjectNode file = MAPPER.createObjectNode();
    file.put("type", "INTERNAL");
    // A reference to internal data carries exactly one ID.
    file.putArray("contents").add(FILE_ID).add(OTHER_FILE_ID);
    final Stream<? extends ExternalId> ids =
        InputType.FILE.asList().apply(new ExtractInputExternalIds(MAPPER, listOf(file), resolver));

    assertThrows(IllegalArgumentException.class, ids::toList);
    assertEquals(0, resolver.calls());
  }

  @Test
  public void whenFileResolves_thenItsExternalKeysAreReturned() {
    final CountingResolver resolver =
        new CountingResolver(Optional.of(metadataFor("pinery-miso", "1234_1_LIB1234")));
    final List<? extends ExternalId> ids =
        InputType.FILE
            .asList()
            .apply(new ExtractInputExternalIds(MAPPER, listOf(internalFile(FILE_ID)), resolver))
            .toList();

    assertEquals(1, ids.size());
    assertEquals("pinery-miso", ids.get(0).getProvider());
    assertEquals("1234_1_LIB1234", ids.get(0).getId());
  }

  private static ObjectNode internalFile(String id) {
    final ObjectNode file = MAPPER.createObjectNode();
    file.put("type", "INTERNAL");
    file.putArray("contents").add(id);
    return file;
  }

  private static ArrayNode listOf(ObjectNode file) {
    return MAPPER.createArrayNode().add(file);
  }

  private static FileMetadata metadataFor(String provider, String id) {
    return new FileMetadata() {
      @Override
      public Stream<ExternalMultiVersionKey> externalKeys() {
        return Stream.of(
            new ExternalMultiVersionKey(provider, id, Map.of("pinery-hash-2", Set.of("a1a1a1a1"))));
      }

      @Override
      public String path() {
        return "/analysis/archive/whatever.fastq.gz";
      }
    };
  }
}
