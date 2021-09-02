package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@RunWith(MockitoJUnitRunner.class)
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

  private static ScheduledExecutorService mockExecutor =
      Mockito.mock(ScheduledExecutorService.class);
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
        new DatabaseBackedProcessor(mockExecutor, new HikariDataSource(dbConfig)) {
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
  public void testGetHashFromAnalysisId() {
    var analysisIdentifier =
        "vidarr:test/file/fa270cc072affa270cc072affa270cc072affa270cc072affa270cc072af";
    Assert.assertEquals(
        "fa270cc072affa270cc072affa270cc072affa270cc072affa270cc072af",
        sut.hashFromAnalysisId(analysisIdentifier));
  }

  @Test
  public void testValidateLabels_validLabels() {
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.STRING);
    var validated = sut.validateLabels(providedLabels, expectedLabels).collect(Collectors.toSet());
    var expected = new HashSet<String>();
    Assert.assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsData() {
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    expectedLabels.put("tumor_group", BasicType.BOOLEAN);
    var validated = sut.validateLabels(providedLabels, expectedLabels).collect(Collectors.toSet());
    var expected = new HashSet<String>();
    expected.add("Label tumor_group: Label: tumor_group: Expected Boolean but got \"first\".");
    Assert.assertEquals(expected, validated);
  }

  @Test
  public void testValidateLabels_invalidLabelsCount() {
    var providedLabels = sut.mapper().createObjectNode();
    providedLabels.put("reference", "hg38");
    providedLabels.put("tumor_group", "first");
    var expectedLabels = new HashMap<String, BasicType>();
    expectedLabels.put("reference", BasicType.STRING);
    var validated = sut.validateLabels(providedLabels, expectedLabels).collect(Collectors.toSet());
    var expected = new HashSet<String>();
    expected.add("2 labels are provided but 1 are expected.");
    Assert.assertEquals(expected, validated);
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
    var externalIds = new HashSet<ExternalMultiVersionKey>();
    var ekv1 = new HashMap<String, Set<String>>();
    var ekvv1 = new HashSet<String>();
    ekvv1.add("a1a1a1a1a1a1a1a1");
    ekvv1.add("b2b2b2b2b2b2b2b2");
    ekv1.put("pinery-hash-22", ekvv1);
    var ek = new ExternalMultiVersionKey("pinery-miso", "1234_1_LIB1234");
    ek.setVersions(ekv1);
    externalIds.add(ek);

    var compute1 =
        sut.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalIds);
    var compute2 =
        sut.computeWorkflowRunHashId(
            workflowName, providedLabels, expectedLabels, inputIds, externalIds);
    Assert.assertEquals(compute1, compute2);
  }
}
