package ca.on.oicr.gsi.vidarr.server.dto;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.EXTERNAL_ID;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.EXTERNAL_ID_VERSION;

import java.util.Set;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

public enum VersionPolicy {
  ALL {
    @Override
    public Field<?> createQuery(Set<String> allowedTypes) {
      var condition = EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID);
      if (allowedTypes != null) {
        condition = condition.and(EXTERNAL_ID_VERSION.KEY.in(allowedTypes));
      }
      final var externalIdVersionAlias = EXTERNAL_ID_VERSION.as("externalIdVersionInner");
      final var table =
          DSL.selectDistinct(EXTERNAL_ID_VERSION.KEY.as("desired_key"))
              .from(EXTERNAL_ID_VERSION)
              .where(condition.and(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.eq(EXTERNAL_ID.ID)))
              .asTable("keys");

      return DSL.field(
          DSL.select(
                  DSL.jsonObjectAgg(
                      DSL.jsonEntry(
                          table.field(0, String.class),
                          DSL.field(
                              DSL.select(DSL.jsonArrayAgg(externalIdVersionAlias.VALUE))
                                  .from(externalIdVersionAlias)
                                  .where(
                                      externalIdVersionAlias
                                          .KEY
                                          .eq(table.field(0, String.class))
                                          .and(
                                              externalIdVersionAlias.EXTERNAL_ID_ID.eq(
                                                  EXTERNAL_ID.ID)))))))
              .from(table));
    }
  },
  LATEST {
    @Override
    public Field<?> createQuery(Set<String> allowedTypes) {
      var condition = EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID);
      if (allowedTypes != null) {
        condition = condition.and(EXTERNAL_ID_VERSION.KEY.in(allowedTypes));
      }
      final var externalIdVersionAlias = EXTERNAL_ID_VERSION.as("externalIdVersionInner");
      return DSL.field(
          DSL.select(
                  DSL.jsonObjectAgg(
                      DSL.jsonEntry(
                          EXTERNAL_ID_VERSION.KEY,
                          DSL.field(
                              DSL.select(
                                      DSL.lastValue(externalIdVersionAlias.VALUE)
                                          .over()
                                          .orderBy(externalIdVersionAlias.CREATED))
                                  .from(externalIdVersionAlias)
                                  .where(
                                      externalIdVersionAlias
                                          .KEY
                                          .eq(EXTERNAL_ID_VERSION.KEY)
                                          .and(
                                              externalIdVersionAlias.EXTERNAL_ID_ID.eq(
                                                  EXTERNAL_ID.ID)))))))
              .from(EXTERNAL_ID_VERSION)
              .where(condition.and(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.eq(EXTERNAL_ID.ID)))
              .groupBy(EXTERNAL_ID_VERSION.KEY));
    }
  },
  NONE {
    @Override
    public Field<?> createQuery(Set<String> allowedTypes) {
      return DSL.inline(null, SQLDataType.JSON);
    }
  };

  public abstract Field<?> createQuery(Set<String> allowedTypes);
}
