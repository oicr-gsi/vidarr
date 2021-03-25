package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.util.postgres.PostgresDSL;

import javax.xml.stream.XMLStreamException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class NiassaWorkflowEngine implements WorkflowEngine {
    private final String dbUrl, dbUser, dbPass;
    private final Set<String> annotationsToKeep;
    private static HikariDataSource pgDataSource;

    static final ObjectMapper MAPPER = new ObjectMapper();

    public NiassaWorkflowEngine(String dbUrl, String dbUser, String dbPass, Set<String> annotationsToKeep) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;

        // TODO: Ensure all the annotations to keep are valid annotations. Maybe in the Provider?
        this.annotationsToKeep = annotationsToKeep;
    }

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

    @Override
    public void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
        monitor.scheduleTask(() -> monitor.permanentFailure(null));
    }

    @Override
    public void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor) {
        // nah
    }

    /**
     * format:
     * {
     *   "migration": [
     *     {
     *        "fileSWID": ${fileSWID},
     *        "output": {
     *          "labels": { "niassa-file-accession": "${fileSWID}", ...file annotations},
     *          "md5": "${MD5}",
     *          "fileSize": ${fileSize},
     *          "path": "${path}",
     *          "metatype": ${metatype}
     *        }
     *   ]
     * }
     */
    @Override
    public JsonNode run(WorkflowLanguage workflowLanguage,
                        String workflow,
                        Stream<Pair<String, String>> accessoryFiles,
                        String vidarrId,
                        ObjectNode workflowParameters,
                        JsonNode engineParameters,
                        WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
        monitor.scheduleTask(()->{
            ObjectNode migration = MAPPER.createObjectNode();
            ArrayNode migrationArray = MAPPER.createArrayNode();

            // Get workflow run SWID from workflowParameters
            final String workflowRunSWID = workflowParameters.get("workflowRunSWID").asText();

            // Set up Postgres config if it has not been configured already
            if(pgDataSource == null){
                HikariDataSource pgDataSource = new HikariDataSource();
                HikariConfig config = new HikariConfig(); // this doesn't get used but I am scared to delete it
                pgDataSource.setJdbcUrl(dbUrl);
                pgDataSource.setUsername(dbUser);
                pgDataSource.setPassword(dbPass);
                this.pgDataSource = pgDataSource;
            }

            // Ask Niassa database for workflow run's analysis provenance
            try(final Connection connection = pgDataSource.getConnection()){
                DSLContext context = PostgresDSL.using(connection, SQLDialect.POSTGRES);
                org.jooq.Result<Record> results = context.fetch(apQuery(workflowRunSWID));

                // Get elements from each result and put into JSON
                for (Record result: results){
                    String fileSWID = result.get(PostgresDSL.field("fileId")).toString();
                    ObjectNode file = MAPPER.createObjectNode()
                            .put("fileSWID", fileSWID);
                    ObjectNode output = MAPPER.createObjectNode()
                            .put("md5", result.get(PostgresDSL.field("fileMd5sum")).toString())
                            .put("fileSize", result.get(PostgresDSL.field("fileSize")).toString())
                            .put("path", result.get(PostgresDSL.field("filePath")).toString())
                            .put("metatype", result.get(PostgresDSL.field("fileMetaType")).toString());
                    ObjectNode labels = MAPPER.createObjectNode()
                            .put("niassa-file-accession", fileSWID);
                    for(String annotation: annotationsToKeep){
                        labels.put(annotation, result.get(PostgresDSL.field(annotation)).toString());
                    }
                    output.set("labels", labels); // They want you to use 'set' instead of 'put' when it's a JsonNode
                    file.set("output", output);
                    migrationArray.add(file);
                }
                migration.set("migration", migrationArray);

                // rtmp://niassa.horse is not a real URL please do not attempt to connect
                monitor.complete(new Result<>(migration, "rtmp://niassa.horse", Optional.empty()));
            } catch(SQLException sqle){
                throw new RuntimeException(sqle);
            }
        });

        return null;
    }

    @Override
    public boolean supports(WorkflowLanguage language) {
        return language == WorkflowLanguage.NIASSA;
    }

    /*
    I have no idea if I'm doing these arrayToString(arrayAgg()) bits properly!
     */
    private ResultQuery apQuery(String workflowRunSWID) {
       return PostgresDSL.select(
                PostgresDSL.coalesce(PostgresDSL.field("pius.lims_ids"), PostgresDSL.field("wrius.lims_ids")).as("iusLimsKeys"),
                PostgresDSL.coalesce(PostgresDSL.field("pius.ius_attributes"), PostgresDSL.field("wrius.ius_attributes")).as("iusAttributes"),
                PostgresDSL.coalesce(PostgresDSL.field("pff.update_tstmp"), PostgresDSL.field("wr.update_tstmp"), PostgresDSL.field("wrius.update_tstmp")).as("lastModified"),
                PostgresDSL.field("w.NAME").as("workflowName"),
                PostgresDSL.field("w.version").as("workflowVersion"),
                PostgresDSL.field("w.sw_accession").as("workflowId"),
                PostgresDSL.field("w_attrs.attrs").as("workflowAttributes"),
                PostgresDSL.field("wr.NAME").as("workflowRunName"),
                PostgresDSL.field("wr.status").as("workflowRunStatus"),
                PostgresDSL.field("wr.sw_accession").as("workflowRunId"),
                PostgresDSL.field("wr_attrs.attrs").as("workflowRunAttributes"),
                PostgresDSL.field("wrifs.swids").as("workflowRunInputFileIds"),
                PostgresDSL.field("pff.processing_algorithm").as("processingAlgorithm"),
                PostgresDSL.field("pff.processing_swid").as("processingId"),
                PostgresDSL.field("pff.processing_status").as("processingStatus"),
                PostgresDSL.field("pff.processing_attrs").as("processingAttributes"),
                PostgresDSL.field("pff.file_meta_type").as("fileMetaType"),
                PostgresDSL.field("pff.file_swid").as("fileId"),
                PostgresDSL.field("pff.file_path").as("filePath"),
                PostgresDSL.field("pff.file_md5sum").as("fileMd5sum"),
                PostgresDSL.field("pff.file_size").as("fileSize"),
                PostgresDSL.field("pff.file_description").as("fileDescription"),
                PostgresDSL.field("pff.file_attrs").as("fileAttributes"),
                PostgresDSL.coalesce(PostgresDSL.field("pius.skip"), PostgresDSL.field("wrius.skip")).as("skip")
        )
            .from(
                    PostgresDSL.select(
                            PostgresDSL.when(PostgresDSL.arrayAgg(PostgresDSL.field("i.sw_accession")).isNull(), PostgresDSL.field("NULL")) // TODO: should that {NULL} be some kind of jooq thing?
                            .otherwise(
                                    PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("i.sq_accession,lk.provider,lk.id,lk.version,lk.modified")), ";") // TODO: ??????
                            ).as("lims_ids"),
                            PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("ia.tag=ia.value")),";").as("ius_attributes"), // TODO: Also ????
                            PostgresDSL.field("iwr.workflow_run_id").as("workflow_run_id"),
                            PostgresDSL.max(PostgresDSL.field("i.update_tstmp")).as("update_tstmp"),
                            PostgresDSL.boolOr((Condition) PostgresDSL.field("i.skip")).as("skip") //TODO: unsure if this is right
                    ).from(
                            PostgresDSL.table("ius").as("i")
                    ).rightOuterJoin(PostgresDSL.table("lims_key").as("lk")).on(PostgresDSL.field("i.lims_key_id").eq(PostgresDSL.field("lk.lims_key_id")))
                    .leftJoin(PostgresDSL.table("ius_attribute").as("ia")).on(PostgresDSL.field("i.ius_id").eq(PostgresDSL.field("ia.ius_id")))
                    .leftJoin(PostgresDSL.table("ius_workflow_runs").as("iwr")).on(PostgresDSL.field("i.ius_id").eq(PostgresDSL.field("iwr.ius_id")))
                    .groupBy(
                            PostgresDSL.field("iwr.workflow_run_id"),
                            PostgresDSL.when(PostgresDSL.field("iwr.workflow_run_id").isNull(), PostgresDSL.field("i.ius_id"))
                                    .otherwise(0)
                    ).asTable("wrius"))
            .leftJoin(PostgresDSL.table("workflow_run").as("wr")).on(PostgresDSL.field("wr.workflow_run_id").eq(PostgresDSL.field("wrius.workflow_run_id")))
                .leftJoin(PostgresDSL.table("workflow").as("w")).on(PostgresDSL.field("wr.workflow_run_id").eq(PostgresDSL.field("w.workflow_id")))
                .leftJoin(
                        PostgresDSL.select(
                                PostgresDSL.field("wr.workflow_run_id").as("workflow_run_id"),
                                PostgresDSL.field("w.workflow_id").as("workflow_id"),
                                PostgresDSL.field("p.update_tstmp"),
                                PostgresDSL.field("p.algorithm").as("processing_algorithm"),
                                PostgresDSL.field("p.sw_accession").as("processing_swid"),
                                PostgresDSL.field("p.processing_id").as("processing_id"),
                                PostgresDSL.field("p.status").as("processing_status"),
                                PostgresDSL.select(
                                        PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("tag=value")), ";")
                                ).from(PostgresDSL.table("processing_attribute"))
                                .where(PostgresDSL.field("p.processing_id").eq(PostgresDSL.field("processing_id")))
                                .groupBy(PostgresDSL.field("processing_id"))
                                        .asField("processing_attrs"),
                                PostgresDSL.field("f.meta_type").as("file_meta_type"),
                                PostgresDSL.field("f.sw_accession").as("file_swid"),
                                PostgresDSL.field("f.file_path").as("file_path"),
                                PostgresDSL.field("f.md5sum").as("file_md5sum"),
                                PostgresDSL.field("f.size").as("file_size"),
                                PostgresDSL.field("f.description").as("file_description"),
                                PostgresDSL.select(
                                        PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("tag=value")), ";")
                                ).from(PostgresDSL.table("file_attribute"))
                                .where(PostgresDSL.field("f.file_id").eq(PostgresDSL.field("file_id")))
                                .groupBy(PostgresDSL.field("file_id"))
                                        .asField("file_attrs")
                        ).from(PostgresDSL.table("processing").as("p"))
                        .rightOuterJoin(PostgresDSL.table("processing_files").as("pf")).on(PostgresDSL.field("p.processing_id").eq(PostgresDSL.field("pf.processing_id")))
                        .rightOuterJoin(PostgresDSL.table("FILE").as("f")).on(PostgresDSL.field("pf.file_id").eq(PostgresDSL.field("f.file_id")))
                        .leftJoin(PostgresDSL.table("workflow_run").as("wr")).on(PostgresDSL.field("p.workflow_run_id").eq(PostgresDSL.field("wr.workflow_run_id")).or(PostgresDSL.field("p.ancestor_workflow_run_id").eq(PostgresDSL.field("wr.workflow_run_id"))))
                        .leftJoin(PostgresDSL.table("workflow").as("w")).on(PostgresDSL.field("wr.workflow_id").eq(PostgresDSL.field("w.workflow_id")))
                        .asTable("pff")
                ).on(PostgresDSL.field("pff.workflow_run_id").eq(PostgresDSL.field("wr.workflow_run_id")))
                .leftJoin(
                        PostgresDSL.select(
                                PostgresDSL.field("pi.processing_id"),
                                PostgresDSL.when(PostgresDSL.arrayAgg(PostgresDSL.field("i.sw_accession")).isNull(), PostgresDSL.field("NULL"))
                                .otherwise(PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("i.sw_accession,lk.provider,lk.id,lk.version,lk.last_modified")), ";")
                                        .as("ius_attributes")),
                                PostgresDSL.boolOr((Condition) PostgresDSL.field("i.skip")).as("skip")
                        ).from(
                                PostgresDSL.table("processing_ius").as("pi")
                        ).leftJoin(PostgresDSL.table("ius").as("i")).on(PostgresDSL.field("pi.ius_id").eq(PostgresDSL.field("i.ius_id")))
                        .leftJoin(PostgresDSL.table("lims_key").as("lk")).on(PostgresDSL.field("i.lims_key_id").eq(PostgresDSL.field("lk.lims_key_id")))
                        .leftJoin(PostgresDSL.table("ius_attribute").as("ia")).on(PostgresDSL.field("i.ius_id").eq(PostgresDSL.field("ia.ius_id")))
                        .groupBy(PostgresDSL.field("pi.processing_id"))
                        .asTable("pius")
                ).on(PostgresDSL.field("pff.processing_id").eq(PostgresDSL.field("pius.processing_id")))
                .leftJoin(
                        PostgresDSL.select(
                                PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("f.sw_accession")),",").as("swids")
                        ).from(
                                PostgresDSL.table("workflow_run_input_files").as("wrif")
                        ).leftJoin(PostgresDSL.table("FILE").as("f")).on(PostgresDSL.field("wrif.file_id").eq(PostgresDSL.field("f.file_id")))
                        .groupBy(PostgresDSL.field("wrif.workflow_run_id"))
                        .asTable("wrifs")
                ).on(PostgresDSL.field("wr.workflow_run_id").eq(PostgresDSL.field("wrifs.workflow_run_id")))
                .leftJoin(
                        PostgresDSL.select(
                                PostgresDSL.field("workflow_run_id"),
                                PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("tag=value")), ";").as("attrs")
                        ).from(PostgresDSL.table("workflow_run_attribute"))
                        .groupBy(PostgresDSL.field("workflow_run_id"))
                        .asTable("wr_attrs")
                ).on(PostgresDSL.field("wrius.workflow_run_id").eq(PostgresDSL.field("wr_attrs.workflow_run_id")))
                .leftJoin(
                        PostgresDSL.select(
                                PostgresDSL.field("workflow_id"),
                                PostgresDSL.arrayToString(PostgresDSL.arrayAgg(PostgresDSL.field("tag=value")), ";").as("attrs")
                        ).from(PostgresDSL.table("workflow_attribute"))
                        .groupBy(PostgresDSL.field("workflow_id"))
                        .asTable("w_attrs")
                ).on(PostgresDSL.field("w.workflow_id").eq(PostgresDSL.field("w_attrs.workflow_id")))
                .where(
                        PostgresDSL.field("wr.sw_accession").isNotNull()
                        .and(PostgresDSL.field("pff.file_swid").isNotNull())
                        .and(PostgresDSL.field("w.NAME").eq(workflowRunSWID))
                );
    }
}
