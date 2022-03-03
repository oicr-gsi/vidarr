package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.util.postgres.PostgresDSL;

public class NiassaWorkflowEngine implements WorkflowEngine {
  private static final String AP_QUERY =
      " SELECT COALESCE(pius.lims_ids, wrius.lims_ids)                        AS \"iusLimsKeys\",\n"
          + "       COALESCE(pius.ius_attributes, wrius.ius_attributes)             AS \"iusAttributes\",\n"
          + "       --       greatest(wr.update_tstmp, pff.update_tstmp, w.update_tstmp) AS \"lastModified\",\n"
          + "       COALESCE(pff.update_tstmp, wr.update_tstmp, wrius.update_tstmp) AS \"lastModified\",-- rename to createdTstmp\n"
          + "       w.NAME                                                          AS \"workflowName\",\n"
          + "       w.version                                                       AS \"workflowVersion\",\n"
          + "       w.sw_accession                                                  AS \"workflowId\",\n"
          + "       w_attrs.attrs                                                   AS \"workflowAttributes\",\n"
          + "       wr.NAME                                                         AS \"workflowRunName\",\n"
          + "       wr.status                                                       AS \"workflowRunStatus\",\n"
          + "       wr.sw_accession                                                 AS \"workflowRunId\",\n"
          + "       wr_attrs.attrs                                                  AS \"workflowRunAttributes\",\n"
          + "       wrifs.swids                                                     AS \"workflowRunInputFileIds\",\n"
          + "       pff.processing_algorithm                                        AS \"processingAlgorithm\",\n"
          + "       pff.processing_swid                                             AS \"processingId\",\n"
          + "       pff.processing_status                                           AS \"processingStatus\",\n"
          + "       pff.processing_attrs                                            AS \"processingAttributes\",\n"
          + "       pff.file_meta_type                                              AS \"fileMetaType\",\n"
          + "       pff.file_swid                                                   AS \"fileId\",\n"
          + "       pff.file_path                                                   AS \"filePath\",\n"
          + "       pff.file_md5sum                                                 AS \"fileMd5sum\",\n"
          + "       pff.file_size                                                   AS \"fileSize\",\n"
          + "       pff.file_description                                            AS \"fileDescription\",\n"
          + "       pff.file_attrs                                                  AS \"fileAttributes\",\n"
          + "       COALESCE(pius.skip, wrius.skip)                                 AS \"skip\"\n"
          + "FROM   (SELECT CASE\n"
          + "                 WHEN ARRAY_AGG(i.sw_accession) = '{NULL}' THEN NULL\n"
          + "                 ELSE ARRAY_TO_STRING(ARRAY_AGG(i.sw_accession\n"
          + "                                                || ','\n"
          + "                                                || lk.provider\n"
          + "                                                || ','\n"
          + "                                                || lk.id\n"
          + "                                                || ','\n"
          + "                                                || lk.version\n"
          + "                                                || ','\n"
          + "                                                || lk.last_modified), ';')\n"
          + "               END                                          AS lims_ids,\n"
          + "               ARRAY_TO_STRING(ARRAY_AGG(ia.tag\n"
          + "                                         || '='\n"
          + "                                         || ia.value), ';') AS ius_attributes,\n"
          + "               iwr.workflow_run_id                          AS workflow_run_id,\n"
          + "               Max(i.update_tstmp)                          AS update_tstmp,\n"
          + "               BOOL_OR(i.skip)                              AS skip\n"
          + "        FROM   ius i\n"
          + "               RIGHT OUTER JOIN lims_key lk\n"
          + "                             ON i.lims_key_id = lk.lims_key_id\n"
          + "               LEFT JOIN ius_attribute ia\n"
          + "                      ON i.ius_id = ia.ius_id\n"
          + "               LEFT JOIN ius_workflow_runs iwr\n"
          + "                      ON i.ius_id = iwr.ius_id\n"
          + "        GROUP  BY iwr.workflow_run_id,\n"
          + "                  CASE\n"
          + "                    WHEN iwr.workflow_run_id IS NULL THEN i.ius_id\n"
          + "                    ELSE 0\n"
          + "                  END) AS wrius\n"
          + "       LEFT JOIN workflow_run wr\n"
          + "              ON wr.workflow_run_id = wrius.workflow_run_id\n"
          + "       LEFT JOIN workflow w\n"
          + "              ON wr.workflow_id = w.workflow_id\n"
          + "       LEFT JOIN (SELECT wr.workflow_run_id        workflow_run_id,\n"
          + "                         w.workflow_id             workflow_id,\n"
          + "                         p.update_tstmp,\n"
          + "                         p.algorithm               processing_algorithm,\n"
          + "                         p.sw_accession            processing_swid,\n"
          + "                         p.processing_id           processing_id,\n"
          + "                         p.status                  processing_status,\n"
          + "                         (SELECT ARRAY_TO_STRING(ARRAY_AGG(tag\n"
          + "                                                           || '='\n"
          + "                                                           || value), ';')\n"
          + "                          FROM   processing_attribute\n"
          + "                          WHERE  p.processing_id = processing_id\n"
          + "                          GROUP  BY processing_id) AS processing_attrs,\n"
          + "                         f.meta_type               file_meta_type,\n"
          + "                         f.sw_accession            file_swid,\n"
          + "                         f.file_path               file_path,\n"
          + "                         f.md5sum                  file_md5sum,\n"
          + "                         f.size                    file_size,\n"
          + "                         f.description             file_description,\n"
          + "                         (SELECT ARRAY_TO_STRING(ARRAY_AGG(tag\n"
          + "                                                           || '='\n"
          + "                                                           || value), ';')\n"
          + "                          FROM   file_attribute\n"
          + "                          WHERE  f.file_id = file_id\n"
          + "                          GROUP  BY file_id)       file_attrs\n"
          + "                  FROM   processing p\n"
          + "                         RIGHT OUTER JOIN processing_files pf\n"
          + "                                       ON ( p.processing_id = pf.processing_id )\n"
          + "                         RIGHT OUTER JOIN FILE f\n"
          + "                                       ON ( pf.file_id = f.file_id )\n"
          + "                         LEFT JOIN workflow_run wr\n"
          + "                                ON ( p.workflow_run_id = wr.workflow_run_id\n"
          + "                                      OR p.ancestor_workflow_run_id = wr.workflow_run_id )\n"
          + "                         LEFT JOIN workflow w\n"
          + "                                ON ( wr.workflow_id = w.workflow_id )) AS pff\n"
          + "              ON pff.workflow_run_id = wr.workflow_run_id\n"
          + "       LEFT JOIN (SELECT pi.processing_id,\n"
          + "                         CASE\n"
          + "                           WHEN ARRAY_AGG(i.sw_accession) = '{NULL}' THEN NULL\n"
          + "                           ELSE ARRAY_TO_STRING(ARRAY_AGG(i.sw_accession\n"
          + "                                                          || ','\n"
          + "                                                          || lk.provider\n"
          + "                                                          || ','\n"
          + "                                                          || lk.id\n"
          + "                                                          || ','\n"
          + "                                                          || lk.version\n"
          + "                                                          || ','\n"
          + "                                                          || lk.last_modified), ';')\n"
          + "                         END                                          lims_ids,\n"
          + "                         ARRAY_TO_STRING(ARRAY_AGG(ia.tag\n"
          + "                                                   || '='\n"
          + "                                                   || ia.value), ';') ius_attributes,\n"
          + "                         BOOL_OR(i.skip)                              AS skip\n"
          + "                  FROM   processing_ius pi\n"
          + "                         LEFT JOIN ius i\n"
          + "                                ON pi.ius_id = i.ius_id\n"
          + "                         LEFT JOIN lims_key lk\n"
          + "                                ON i.lims_key_id = lk.lims_key_id\n"
          + "                         LEFT JOIN ius_attribute ia\n"
          + "                                ON i.ius_id = ia.ius_id\n"
          + "                  GROUP  BY pi.processing_id) AS pius\n"
          + "              ON pff.processing_id = pius.processing_id\n"
          + "       LEFT JOIN (SELECT wrif.workflow_run_id,\n"
          + "                         ARRAY_TO_STRING(ARRAY_AGG(f.sw_accession), ',') swids\n"
          + "                  FROM   workflow_run_input_files wrif\n"
          + "                         LEFT JOIN FILE f\n"
          + "                                ON wrif.file_id = f.file_id\n"
          + "                  GROUP  BY wrif.workflow_run_id) AS wrifs\n"
          + "              ON wr.workflow_run_id = wrifs.workflow_run_id\n"
          + "       LEFT JOIN (SELECT workflow_run_id,\n"
          + "                         ARRAY_TO_STRING(ARRAY_AGG(tag\n"
          + "                                                   || '='\n"
          + "                                                   || value), ';') attrs\n"
          + "                  FROM   workflow_run_attribute\n"
          + "                  GROUP  BY workflow_run_id) wr_attrs\n"
          + "              ON wrius.workflow_run_id = wr_attrs.workflow_run_id\n"
          + "       LEFT JOIN (SELECT workflow_id,\n"
          + "                         ARRAY_TO_STRING(ARRAY_AGG(tag\n"
          + "                                                   || '='\n"
          + "                                                   || value), ';') attrs\n"
          + "                  FROM   workflow_attribute\n"
          + "                  GROUP  BY workflow_id) w_attrs\n"
          + "              ON w.workflow_id = w_attrs.workflow_id\n"
          + " WHERE wr.sw_accession = %s AND pff.file_swid IS NOT NULL";
  static final ObjectMapper MAPPER = new ObjectMapper();

  public static WorkflowEngineProvider provider() {
    return () -> Stream.of(new Pair<>("niassa", NiassaWorkflowEngine.class));
  }

  private Set<String> annotations;
  private String dbUrl, dbName, dbUser, dbPass;
  @JsonIgnore private HikariDataSource pgDataSource;

  public NiassaWorkflowEngine() {}

  @Override
  public JsonNode cleanup(JsonNode cleanupState, WorkMonitor<Void, JsonNode> monitor) {
    monitor.scheduleTask(() -> monitor.complete(null));
    return null;
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    // nah
  }

  @Override
  public Optional<BasicType> engineParameters() {
    return Optional.empty();
  }

  public Set<String> getAnnotations() {
    return annotations;
  }

  public String getDbName() {
    return dbName;
  }

  public String getDbPass() {
    return dbPass;
  }

  public String getDbUrl() {
    return dbUrl;
  }

  public String getDbUser() {
    return dbUser;
  }

  @Override
  public void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
    monitor.scheduleTask(() -> monitor.permanentFailure(null));
  }

  @Override
  public void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor) {
    // nah
  }

  /**
   *
   *
   * <pre>
   * format:
   *   { migration": [
   *     { "fileSWID": ${fileSWID},
   *     "fileMetadata":
   *       { "right":
   *         { "niassa-file-accession": "${fileSWID}",
   *         ...file annotations},
   *       "left": "{ "md5": "${MD5}", "fileSize": ${fileSize}, "path": "${path}", "metatype": ${metatype} }" }
   *     ]
   *   }
   *   </pre>
   */
  @Override
  public JsonNode run(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
    monitor.scheduleTask(
        () -> {
          ObjectNode migration = MAPPER.createObjectNode();
          ArrayNode migrationArray = MAPPER.createArrayNode();

          // Get workflow run SWID from workflowParameters
          final String workflowRunSWID = workflowParameters.get("workflowRunSWID").asText();

          // Ask Niassa database for workflow run's analysis provenance
          try (final Connection connection = pgDataSource.getConnection()) {
            DSLContext context = PostgresDSL.using(connection, SQLDialect.POSTGRES);
            org.jooq.Result<Record> results =
                context.fetch(String.format(AP_QUERY, workflowRunSWID));

            // Get elements from each result and put into JSON
            Map<String, Integer> pathAndFileSwid = new HashMap<>();
            for (Record result : results) {
              String filePath = result.get(PostgresDSL.field("filePath")).toString();
              String fileSWID = result.get(PostgresDSL.field("fileId")).toString();
              // check if we've already seen this file path - we only want one file path, the one
              // with the max (most recent) fileSwid
              var currentFileSwidInt = Integer.parseInt(fileSWID);
              var swidForPreviouslySeenFilePathInt = pathAndFileSwid.get(filePath);
              if (swidForPreviouslySeenFilePathInt != null
                  && currentFileSwidInt < swidForPreviouslySeenFilePathInt) {
                // current fileSwid is older than previously-processed fileSwid, so ignore it
                continue;
              } else if (swidForPreviouslySeenFilePathInt != null
                  && currentFileSwidInt > swidForPreviouslySeenFilePathInt) {
                // current fileSwid is newer than previously-processed fileSwid, so remove the old
                // fileSwid and all its file details
                for (int i = 0; i < migrationArray.size(); i++) {
                  if (migrationArray
                      .get(i)
                      .get("fileSWID")
                      .textValue()
                      .equals(String.valueOf(swidForPreviouslySeenFilePathInt))) {
                    migrationArray.remove(i);
                    break;
                  }
                }
              }
              // add the current fileSwid details
              pathAndFileSwid.put(filePath, currentFileSwidInt);

              ObjectNode file = MAPPER.createObjectNode().put("fileSWID", fileSWID);
              ObjectNode left =
                  MAPPER
                      .createObjectNode()
                      .put("md5", result.get(PostgresDSL.field("fileMd5sum")).toString())
                      .put("fileSize", result.get(PostgresDSL.field("fileSize")).toString())
                      .put("path", filePath)
                      .put("metatype", result.get(PostgresDSL.field("fileMetaType")).toString());
              ObjectNode right = MAPPER.createObjectNode().put("niassa-file-accession", fileSWID);
              Object fileAttributesObj = result.get(PostgresDSL.field("fileAttributes"));
              if (null != fileAttributesObj) {
                String[] fileAttributes = fileAttributesObj.toString().split(";");
                for (String fileAttribute : fileAttributes) {
                  String[] keyAndValue = fileAttribute.split("="); // 0 is key, 1 is value
                  if (annotations.contains(keyAndValue[0])) {
                    right.put(keyAndValue[0], keyAndValue[1]);
                  }
                }
              }
              ObjectNode output =
                  MAPPER
                      .createObjectNode()
                      .set(
                          "right",
                          right); // They want you to use 'set' instead of 'put' when it's a
              // JsonNode
              output.put("left", left.toString());
              file.set("fileMetadata", output);
              migrationArray.add(file);
            }
            migration.set("migration", migrationArray);

            // rtmp://niassa.horse is not a real URL please do not attempt to connect
            monitor.complete(new Result<>(migration, "rtmp://niassa.horse", Optional.empty()));
          } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
          }
        });

    return null;
  }

  @Override
  public void startup() {
    // Set up Postgres config
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(dbUrl);
    config.setDataSourceJNDI(dbName);
    config.setUsername(dbUser);
    config.setPassword(dbPass);
    pgDataSource = new HikariDataSource(config);
  }

  public void setAnnotations(Set<String> annotations) {
    this.annotations = annotations;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setDbPass(String dbPass) {
    this.dbPass = dbPass;
  }

  public void setDbUrl(String dbUrl) {
    this.dbUrl = dbUrl;
  }

  public void setDbUser(String dbUser) {
    this.dbUser = dbUser;
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.NIASSA;
  }
}
