package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.services.DataAssetService;
import de.fraunhofer.fokus.ids.services.FileService;
import de.fraunhofer.fokus.ids.services.RepositoryService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MainVerticle extends AbstractVerticle {
    private static Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(RoutingVerticle.class.getName());
        vertx.deployVerticle(DataAssetService.class.getName());
        vertx.deployVerticle(FileService.class.getName());
        vertx.deployVerticle(RepositoryService.class.getName());
    }
}