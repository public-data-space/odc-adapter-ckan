package de.fraunhofer.fokus.ids.services.database;

import de.fraunhofer.fokus.ids.services.Constants;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.serviceproxy.ServiceBinder;

public class DatabaseServiceVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(DatabaseServiceVerticle.class.getName());

    @Override
    public void start(Future<Void> startFuture) {


        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setConfig(new JsonObject().put("path", this.getClass().getClassLoader().getResource("conf/application.conf").getFile()));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                JsonObject config = ar.result().getJsonObject("config").getJsonObject("database");
                SQLClient jdbc = JDBCClient.createShared(vertx, config);
                DatabaseService.create(jdbc, ready -> {
                    if (ready.succeeded()) {
                        ServiceBinder binder = new ServiceBinder(vertx);
                        binder
                                .setAddress(Constants.DATABASE_SERVICE)
                                .register(DatabaseService.class, ready.result());
                        startFuture.complete();
                    } else {
                        startFuture.fail(ready.cause());
                    }
                });
            } else {
                LOGGER.error("Config could not be retrieved.");
            }
        });
        startFuture.complete();
    }

}
