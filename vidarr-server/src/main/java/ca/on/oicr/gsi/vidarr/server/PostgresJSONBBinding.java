package ca.on.oicr.gsi.vidarr.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Objects;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.postgresql.util.PGobject;
// Taken from
// https://gist.githubusercontent.com/vijayparashar12/e1b61543ace2debb20b6a302fa408eaf/raw/2a78e79c63204ab378b0090c4e40d21867330cee/PostgresJSONBBinding.java

public class PostgresJSONBBinding implements Binding<JSONB, JsonNode> {

  @Override
  public Converter<JSONB, JsonNode> converter() {
    return new Converter<>() {
      @Override
      public JsonNode from(JSONB t) {
        try {
          return t == null
              ? DatabaseBackedProcessor.MAPPER.nullNode()
              : DatabaseBackedProcessor.MAPPER.readValue(t.data(), JsonNode.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public JSONB to(JsonNode u) {
        try {
          return u == null
              ? null
              : JSONB.valueOf(DatabaseBackedProcessor.MAPPER.writeValueAsString(u));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Class<JSONB> fromType() {
        return JSONB.class;
      }

      @Override
      public Class<JsonNode> toType() {
        return JsonNode.class;
      }
    };
  }

  @Override
  public void sql(BindingSQLContext<JsonNode> ctx) throws SQLException {
    ctx.render().visit(DSL.val(ctx.convert(converter()).value())).sql("::jsonb");
  }

  @Override
  public void register(final BindingRegisterContext<JsonNode> ctx) throws SQLException {
    ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
  }

  @Override
  public void set(final BindingSetStatementContext<JsonNode> ctx) throws SQLException {
    final var jsonObject = new PGobject();
    jsonObject.setType("json");
    jsonObject.setValue(Objects.toString(ctx.convert(converter()).value()));
    ctx.statement().setObject(ctx.index(), jsonObject);
  }

  @Override
  public void set(final BindingSetSQLOutputContext<JsonNode> ctx) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void get(final BindingGetResultSetContext<JsonNode> ctx) throws SQLException {
    ctx.convert(converter()).value(JSONB.valueOf(ctx.resultSet().getString(ctx.index())));
  }

  @Override
  public void get(final BindingGetStatementContext<JsonNode> ctx) throws SQLException {
    ctx.convert(converter()).value(JSONB.valueOf(ctx.statement().getString(ctx.index())));
  }

  @Override
  public void get(final BindingGetSQLInputContext<JsonNode> ctx) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
