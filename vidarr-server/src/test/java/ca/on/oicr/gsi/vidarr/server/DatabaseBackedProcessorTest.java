package ca.on.oicr.gsi.vidarr.server;

import static org.junit.Assert.*;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.Target;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class DatabaseBackedProcessorTest {

  @ClassRule
  public static JdbcDatabaseContainer pg =
      DatabaseBackedTestConfiguration.getTestDatabaseContainer();

  private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
  private static HikariConfig dbConfig;
  private DatabaseBackedProcessor sut;

  @BeforeClass
  public static void setup() {
    dbConfig = new HikariConfig();
    dbConfig.setJdbcUrl(
        String.format(
            "jdbc:postgresql://%s:%d/%s",
            pg.getHost(), pg.getFirstMappedPort(), pg.getDatabaseName()));
    dbConfig.setUsername(pg.getUsername());
    dbConfig.setPassword(pg.getPassword());
    dbConfig.setAutoCommit(false);
    dbConfig.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
  }

  @Before
  public void cleanAndMigrateDb() {
    final PGSimpleDataSource simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {pg.getHost()});
    simpleConnection.setPortNumbers(new int[] {pg.getFirstMappedPort()});
    simpleConnection.setDatabaseName(pg.getDatabaseName());
    simpleConnection.setUser(pg.getUsername());
    simpleConnection.setPassword(pg.getPassword());
    FluentConfiguration fw = Flyway.configure().dataSource(simpleConnection).cleanDisabled(false);
    fw.load().clean();
    fw.locations("classpath:db/migration", "classpath:db/testdata").load().migrate();

    sut =
        new DatabaseBackedProcessor(executor, new HikariDataSource(dbConfig)) {
          private Optional<FileMetadata> fetchPathForId(String id) {
            return Optional.empty();
          }

          @Override
          public Optional<FileMetadata> pathForId(String id) {
            return Optional.empty();
          }

          @Override
          protected Optional<Target> targetByName(String name) {
            return Optional.empty();
          }
        };
  }

  @Test
  public void testValidateLabels_validLabels() {
    ObjectNode providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    HashMap<String, BasicType> expectedLabels = new HashMap<>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.STRING);
    Set<String> validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    HashSet<String> expected = new HashSet<>();
    assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsData() {
    ObjectNode providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    HashMap<String, BasicType> expectedLabels = new HashMap<>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.BOOLEAN);
    Set<String> validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    HashSet<String> expected = new HashSet<>();
    expected.add("Label tumor_group: Label: tumor_group: Expected Boolean but got \"first\".");
    assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsCount() {
    ObjectNode providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    HashMap<String, BasicType> expectedLabels = new HashMap<>();
    expectedLabels.put("reference", BasicType.STRING);
    Set<String> validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    HashSet<String> expected = new HashSet<>();
    expected.add("2 labels are provided but 1 are expected.");
    assertEquals(expected, validated);
  }

  @Test
  public void testComputeWorkflowHash_alwaysSameHash() {
    String workflowName = "bcl2fastq";
    ObjectNode providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    TreeSet<String> expectedLabels = new TreeSet<>();
    expectedLabels.add("reference");
    expectedLabels.add("tumor_group");
    TreeSet<String> inputIds = new TreeSet<>();
    inputIds.add("vidarr:test/file/abcdefabcdefabcdef");
    inputIds.add("vidarr:test/file/fedcbafedcbafedcba");
    HashSet<ExternalMultiVersionKey> externalKeys = new HashSet<>();
    HashMap<String, Set<String>> ekv1 = new HashMap<>();
    HashSet<String> ekvv1 = new HashSet<>();
    ekvv1.add("b2b2b2b2b2b2b2b2");
    ekv1.put("pinery-hash-22", ekvv1);
    HashSet<String> ekvv2 = new HashSet<>();
    ekvv2.add("a1a1a1a1a1a1a1a1");
    ekv1.put("shesmu-sha3", ekvv2);
    ExternalMultiVersionKey ek = new ExternalMultiVersionKey("pinery-miso", "1234_1_LIB1234");
    ek.setVersions(ekv1);
    externalKeys.add(ek);

    String compute1 =
        DatabaseBackedProcessor.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalKeys);
    String compute2 =
        DatabaseBackedProcessor.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalKeys);
    assertEquals(compute1, compute2);

    // Confirm that this computes independently of external key versions

    HashSet<ExternalId> externalIds = new HashSet<>();
    ExternalId ei = new ExternalId("pinery-miso", "1234_1_LIB1234");
    externalIds.add(ei);
    String compute3 =
        DatabaseBackedProcessor.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalIds);
    assertEquals(compute1, compute3);
  }

  @Test
  public void testResolveInDatabase_forFileWithSingleExternalIdVersion() {
    FileMetadata expected =
        new FileMetadata() {
          final String fileHashId =
              "916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2";

          @Override
          public String path() {
            return "/analysis/archive/seqware/seqware_analysis_12/hsqwprod/seqware-results/CASAVA_2.9.1/83779816/SWID_14718190_DCRT_016_Br_R_PE_234_MR_obs528_P016_190711_M00146_0072_000000000-D6D3B_ACTGAT_L001_R2_001.fastq.gz";
          }

          @Override
          public Stream<ExternalMultiVersionKey> externalKeys() {
            Map<String, Set<String>> versions = new HashMap<>();
            versions.put(
                "pinery-hash-2",
                Stream.of("bea8063d6c8e66e4c6faae52ddc8e5e7ab249782cb98ec7fb64261f12e82a3bf")
                    .collect(Collectors.toSet()));
            return Stream.of(
                new ExternalMultiVersionKey("pinery-miso", "3786_1_LDI31800", versions));
          }
        };
    FileMetadata metadata =
        sut.resolveInDatabase("916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2")
            .get();
    assertEquals(expected.path(), metadata.path());

    assertEquals(
        getExternalIdInfo(expected, ExternalMultiVersionKey::getId),
        getExternalIdInfo(metadata, ExternalMultiVersionKey::getId));
    assertEquals(
        getExternalIdInfo(expected, ExternalMultiVersionKey::getProvider),
        getExternalIdInfo(metadata, ExternalMultiVersionKey::getProvider));
    assertEquals(getExternalKeyKeys(expected), getExternalKeyKeys(metadata));
    assertEquals(getExternalKeyValues(expected), getExternalKeyValues(metadata));
  }

  @Test
  public void testResolveInDatabase_forFileWithMultipleExternalIdVersions() {
    FileMetadata expected =
        new FileMetadata() {
          final String fileHashId =
              "767d00090277cb760d69352c944a30d252e7950a0e89c6ea1951121e8443389f";

          @Override
          public String path() {
            return "/analysis/archive/seqware/seqware_analysis_8/hsqwprod/results/fastqc_1.0"
                + ".0/85993576/SWID_1414141_AAAA_0001_nn_n_PE_316_MR_NoGroup_150213_D00355_0080_BC5UR0ANXX_ACAGTG_L001_R1_001.fastqc.gz";
          }

          @Override
          public Stream<ExternalMultiVersionKey> externalKeys() {
            Map<String, Set<String>> versions = new HashMap<>();
            versions.put(
                "pinery-hash-2",
                Stream.of("f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2")
                    .collect(Collectors.toSet()));
            versions.put(
                "pinery-hash-7",
                Stream.of("f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7")
                    .collect(Collectors.toSet()));
            versions.put(
                "pinery-hash-8",
                Stream.of(
                        "a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2",
                        "f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8",
                        "f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9")
                    .collect(Collectors.toSet()));
            return Stream.of(
                new ExternalMultiVersionKey("pinery-miso", "5042_1_LDI55100", versions));
          }
        };
    FileMetadata metadata =
        sut.resolveInDatabase("767d00090277cb760d69352c944a30d252e7950a0e89c6ea1951121e8443389f")
            .get();
    assertEquals(expected.path(), metadata.path());

    assertEquals(
        getExternalIdInfo(expected, ExternalMultiVersionKey::getId),
        getExternalIdInfo(metadata, ExternalMultiVersionKey::getId));
    assertEquals(
        getExternalIdInfo(expected, ExternalMultiVersionKey::getProvider),
        getExternalIdInfo(metadata, ExternalMultiVersionKey::getProvider));
    assertEquals(getExternalKeyKeys(expected), getExternalKeyKeys(metadata));
    assertEquals(getExternalKeyValues(expected), getExternalKeyValues(metadata));
  }

  @Test
  public void testResolveInDatabase_invalidFileId() {
    Optional<FileMetadata> metadata = sut.resolveInDatabase("doesNotExist");
    assertFalse(metadata.isPresent());
  }

  /**
   * A file that cannot be resolved makes the extractor throw, and the throw arrives late because the
   * extractor's streams are lazy. Rather than escaping the submission, it has to come back as a
   * reported ID so the caller gets a 400 naming what went wrong.
   */
  @Test
  public void testExtractExternalIds_unresolvableIdIsReportedNotThrown()
      throws ReflectiveOperationException {
    ObjectNode arguments = sut.mapper().createObjectNode();
    arguments.set("fastqs", listOf(internalFile(FASTQ_FILE_ID)));

    TreeSet<String> unresolvedIds = new TreeSet<>();
    TreeSet<ExternalId> externalIds =
        extractExternalIds(
            sut,
            arguments,
            definitionTaking(new WorkflowDefinition.Parameter(InputType.FILE.asList(), "fastqs")),
            unresolvedIds);

    assertTrue(externalIds.isEmpty());
    // The ID is recorded by the resolver before the extractor throws, and the parameter name by the
    // catch, so both name the same failure.
    assertEquals(Set.of(FASTQ_FILE_ID, "fastqs"), unresolvedIds);
  }

  /**
   * Contents that are not a single ID never reach the resolver, so the parameter name is all there
   * is to report.
   */
  @Test
  public void testExtractExternalIds_malformedInternalContentsReportTheParameter()
      throws ReflectiveOperationException {
    ObjectNode file = sut.mapper().createObjectNode();
    file.put("type", "INTERNAL");
    file.putArray("contents").add(FASTQ_FILE_ID).add(FASTQC_FILE_ID);
    ObjectNode arguments = sut.mapper().createObjectNode();
    arguments.set("fastqs", listOf(file));

    TreeSet<String> unresolvedIds = new TreeSet<>();
    TreeSet<ExternalId> externalIds =
        extractExternalIds(
            sut,
            arguments,
            definitionTaking(new WorkflowDefinition.Parameter(InputType.FILE.asList(), "fastqs")),
            unresolvedIds);

    assertTrue(externalIds.isEmpty());
    assertEquals(Set.of("fastqs"), unresolvedIds);
  }

  /**
   * Each parameter is extracted on its own, so one that cannot be understood does not cost the
   * external IDs of the ones that can.
   */
  @Test
  public void testExtractExternalIds_oneBadParameterDoesNotDiscardTheRest()
      throws ReflectiveOperationException {
    DatabaseBackedProcessor processor =
        processorResolving(
            Map.of(FASTQC_FILE_ID, metadataFor("pinery-miso", "3786_1_LDI31800")));

    ObjectNode badFile = processor.mapper().createObjectNode();
    badFile.put("type", "INTERNAL");
    badFile.putArray("contents").add(FASTQ_FILE_ID).add(FASTQC_FILE_ID);
    ObjectNode arguments = processor.mapper().createObjectNode();
    arguments.set("fastqs", listOf(badFile));
    arguments.set("fastqcs", listOf(internalFile(FASTQC_FILE_ID)));

    TreeSet<String> unresolvedIds = new TreeSet<>();
    TreeSet<ExternalId> externalIds =
        extractExternalIds(
            processor,
            arguments,
            definitionTaking(
                new WorkflowDefinition.Parameter(InputType.FILE.asList(), "fastqs"),
                new WorkflowDefinition.Parameter(InputType.FILE.asList(), "fastqcs")),
            unresolvedIds);

    assertEquals(Set.of("fastqs"), unresolvedIds);
    assertEquals(
        Set.of("3786_1_LDI31800"),
        externalIds.stream().map(ExternalId::getId).collect(Collectors.toSet()));
  }

  private static final String FASTQ_FILE_ID =
      "vidarr:test/file/916df707b105ddd88d8979e41208f2507a6d0c8d3ef57677750efa7857c4f6b2";
  private static final String FASTQC_FILE_ID =
      "vidarr:test/file/767d00090277cb760d69352c944a30d252e7950a0e89c6ea1951121e8443389f";

  private static WorkflowDefinition definitionTaking(WorkflowDefinition.Parameter... parameters) {
    return new WorkflowDefinition(
        WorkflowLanguage.UNIX_SHELL,
        "0000000000000000000000000000000000000000000000000000000000000000",
        "#!/bin/sh\necho 'extract me'",
        Collections.emptyMap(),
        Stream.of(parameters),
        Stream.empty());
  }

  /**
   * The only caller of {@code extractExternalIds} is buried in {@code submit} behind a target and
   * every other submission check, so these tests call it directly to stay about the extraction. A
   * {@link DatabaseBackedProcessor.WorkflowInformation} normally only comes from a workflow version
   * row, which is why building one here takes reflection.
   */
  private static TreeSet<ExternalId> extractExternalIds(
      DatabaseBackedProcessor processor,
      JsonNode arguments,
      WorkflowDefinition definition,
      TreeSet<String> unresolvedIds)
      throws ReflectiveOperationException {
    Constructor<DatabaseBackedProcessor.WorkflowInformation> constructor =
        DatabaseBackedProcessor.WorkflowInformation.class.getDeclaredConstructor(
            int.class, WorkflowDefinition.class, SortedMap.class);
    constructor.setAccessible(true);
    return processor.extractExternalIds(
        arguments,
        constructor.newInstance(1, definition, new TreeMap<String, BasicType>()),
        unresolvedIds);
  }

  private ObjectNode internalFile(String id) {
    ObjectNode file = sut.mapper().createObjectNode();
    file.put("type", "INTERNAL");
    file.putArray("contents").add(id);
    return file;
  }

  private JsonNode listOf(ObjectNode file) {
    return sut.mapper().createArrayNode().add(file);
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

  private DatabaseBackedProcessor processorResolving(Map<String, FileMetadata> files) {
    return new DatabaseBackedProcessor(executor, new HikariDataSource(dbConfig)) {
      @Override
      public Optional<FileMetadata> pathForId(String id) {
        return Optional.ofNullable(files.get(id));
      }

      @Override
      protected Optional<Target> targetByName(String name) {
        return Optional.empty();
      }
    };
  }

  private Set<String> getExternalIdInfo(
      FileMetadata fm, Function<ExternalMultiVersionKey, String> fn) {
    return fm.externalKeys().map(fn::apply).collect(Collectors.toSet());
  }

  private Set<String> getExternalKeyKeys(FileMetadata fm) {
    return fm.externalKeys()
        .flatMap(ek -> ek.getVersions().keySet().stream())
        .collect(Collectors.toSet());
  }

  private Set<String> getExternalKeyValues(FileMetadata fm) {
    return fm.externalKeys()
        .flatMap(ek -> ek.getVersions().values().stream())
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }
}
