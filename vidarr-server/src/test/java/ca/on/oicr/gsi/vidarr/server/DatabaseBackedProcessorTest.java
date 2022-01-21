package ca.on.oicr.gsi.vidarr.server;

import static org.junit.Assert.*;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class DatabaseBackedProcessorTest {
  private static final String vidarrTest = "vidarr-test";
  private static final String dbName = vidarrTest;
  private static final String dbUser = vidarrTest;
  private static final String dbPass = vidarrTest;

  @ClassRule
  public static JdbcDatabaseContainer pg =
      new PostgreSQLContainer("postgres:12-alpine")
          .withDatabaseName(dbName)
          .withUsername(dbUser)
          .withPassword(dbPass);

  private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
  private static HikariConfig dbConfig;
  private DatabaseBackedProcessor sut;

  @BeforeClass
  public static void setup() {
    dbConfig = new HikariConfig();
    dbConfig.setJdbcUrl(
        String.format("jdbc:postgresql://%s:%d/%s", pg.getHost(), pg.getFirstMappedPort(), dbName));
    dbConfig.setUsername(dbUser);
    dbConfig.setPassword(dbPass);
    dbConfig.setAutoCommit(false);
    dbConfig.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
  }

  @Before
  public void cleanAndMigrateDb() {
    final var simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {pg.getHost()});
    simpleConnection.setPortNumbers(new int[] {pg.getFirstMappedPort()});
    simpleConnection.setDatabaseName(dbName);
    simpleConnection.setUser(dbUser);
    simpleConnection.setPassword(dbPass);
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
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.STRING);
    var validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    var expected = new HashSet<String>();
    assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsData() {
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.BOOLEAN);
    var validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    var expected = new HashSet<String>();
    expected.add("Label tumor_group: Label: tumor_group: Expected Boolean but got \"first\".");
    assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsCount() {
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    var validated =
        DatabaseBackedProcessor.validateLabels(providedLabels, expectedLabels)
            .collect(Collectors.toSet());
    var expected = new HashSet<String>();
    expected.add("2 labels are provided but 1 are expected.");
    assertEquals(expected, validated);
  }

  @Test
  public void testComputeWorkflowHash_alwaysSameHash() {
    var workflowName = "bcl2fastq";
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new TreeSet<String>();
    expectedLabels.add("reference");
    expectedLabels.add("tumor_group");
    var inputIds = new TreeSet<String>();
    inputIds.add("vidarr:test/file/abcdefabcdefabcdef");
    inputIds.add("vidarr:test/file/fedcbafedcbafedcba");
    var externalKeys = new HashSet<ExternalMultiVersionKey>();
    var ekv1 = new HashMap<String, Set<String>>();
    var ekvv1 = new HashSet<String>();
    ekvv1.add("b2b2b2b2b2b2b2b2");
    ekv1.put("pinery-hash-22", ekvv1);
    var ekvv2 = new HashSet<String>();
    ekvv2.add("a1a1a1a1a1a1a1a1");
    ekv1.put("shesmu-sha3", ekvv2);
    var ek = new ExternalMultiVersionKey("pinery-miso", "1234_1_LIB1234");
    ek.setVersions(ekv1);
    externalKeys.add(ek);

    var compute1 =
        DatabaseBackedProcessor.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalKeys);
    var compute2 =
        DatabaseBackedProcessor.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalKeys);
    assertEquals(compute1, compute2);

    // Confirm that this computes independently of external key versions

    var externalIds = new HashSet<ExternalId>();
    var ei = new ExternalId("pinery-miso", "1234_1_LIB1234");
    externalIds.add(ei);
    var compute3 =
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
