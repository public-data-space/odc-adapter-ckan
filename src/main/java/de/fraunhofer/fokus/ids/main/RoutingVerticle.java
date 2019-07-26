package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.enums.FileType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.http.entity.ContentType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RoutingVerticle extends AbstractVerticle {

    private Logger LOGGER = LoggerFactory.getLogger(RoutingVerticle.class.getName());
    private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";
    private Router router;
    private EventBus eb;

    @Override
    public void start(Future<Void> startFuture) {
        router = Router.router(vertx);
        eb =  vertx.eventBus();

        createHttpServer();

    }

    private void createHttpServer() {
        HttpServer server = vertx.createHttpServer();

        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");
        allowedHeaders.add("X-PINGARUNER");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);

        router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
        router.route().handler(BodyHandler.create());

        router.post("/create")
                .handler(routingContext ->
                        create(result -> reply(result, routingContext.response()), routingContext)
                );

        router.post("/delete")
                .handler(routingContext ->
                        delete(result -> reply(result, routingContext.response()), routingContext)
                );

        router.post("/getFile")
                .handler(routingContext ->
                        getFile(result -> reply(result, routingContext.response()), routingContext)
                );

        router.route("/supported")
                .handler(routingContext ->
                        supported(result -> reply(result, routingContext.response()))
                );

        LOGGER.info("Starting CKAN adapter...");
        server.requestHandler(router).listen(8091);
        LOGGER.info("CKAN adapter successfully started.");
    }

    private void create(Handler<AsyncResult<String>> next, RoutingContext routingContext) {

        eb.send(ROUTE_PREFIX + "ckan.createDataAsset", routingContext.getBodyAsJson().toString(), res -> {
            if(res.succeeded()){
                next.handle(Future.succeededFuture(res.result().body().toString()));
            }
            else{
                next.handle(Future.failedFuture(res.cause().toString()));
            }
        });

    }

    private void delete(Handler<AsyncResult<String>> next, RoutingContext routingContext) {

        eb.send(ROUTE_PREFIX + "ckan.deleteDataAsset", routingContext.getBodyAsJson().toString(), res -> {
            if(res.succeeded()){
                next.handle(Future.succeededFuture(res.result().body().toString()));
            }
            else{
                next.handle(Future.failedFuture(res.cause().toString()));
            }
        });

    }

    private void getFile(Handler<AsyncResult<String>> next, RoutingContext routingContext) {
        eb.send(ROUTE_PREFIX + "ckan.getFile", routingContext.getBodyAsJson().toString(), res -> {
            if(res.succeeded()){
                next.handle(Future.succeededFuture(res.result().body().toString()));
            }
            else{
                next.handle(Future.failedFuture(res.cause().toString()));
            }
        });
    }

    private void supported(Handler<AsyncResult<String>> next) {
        LOGGER.info("Returning supported data formats.");
        List<FileType> types = new ArrayList<>();
        types.add(FileType.JSON);
        types.add(FileType.XML);
        types.add(FileType.TXT);

        next.handle(Future.succeededFuture(Json.encode(types)));
    }

    private void reply(AsyncResult<String> result, HttpServerResponse response){
        if (result.succeeded()) {
            if(result.result() != null) {
                String entity = result.result();
                response.putHeader("content-type", ContentType.APPLICATION_JSON.toString());
                response.end(entity);
            }
            else{
                response.setStatusCode(404).end();
            }
        }
        else {
            response.setStatusCode(404).end();
        }
    }

}
