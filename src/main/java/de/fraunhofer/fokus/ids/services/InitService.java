package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.LoggerFactory;

public class InitService {
    final io.vertx.core.logging.Logger LOGGER = LoggerFactory.getLogger(InitService.class.getName());

    DatabaseService databaseService;

    public InitService(Vertx vertx, Handler<AsyncResult<Void>> resultHandler){
        this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
        initDB(reply -> {
            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture());
            }
            else{
                LOGGER.info("Table creation failed.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }

        });
    }

    private void initDB(Handler<AsyncResult<Void>> resultHandler){
        databaseService.update("CREATE TABLE IF NOT EXISTS accessinformation (created_at, updated_at, dataassetid, filename)", new JsonArray(), reply -> {
            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture());
            }
            else{
                LOGGER.info("Table creation failed.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }
}
