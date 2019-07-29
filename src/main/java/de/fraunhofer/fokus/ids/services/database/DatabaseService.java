package de.fraunhofer.fokus.ids.services.database;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;

import java.util.List;

@ProxyGen
@VertxGen
public interface DatabaseService {

    @Fluent
    DatabaseService query(String query, JsonArray params, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    @Fluent
    DatabaseService update(String query, JsonArray params, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    @GenIgnore
    static DatabaseService create(SQLClient dbClient, Handler<AsyncResult<DatabaseService>> readyHandler) {
        return new DatabaseServiceImpl(dbClient, readyHandler);
    }

    @GenIgnore
    static DatabaseService createProxy(Vertx vertx, String address) {
        return new DatabaseServiceVertxEBProxy(vertx, address);
    }
}
