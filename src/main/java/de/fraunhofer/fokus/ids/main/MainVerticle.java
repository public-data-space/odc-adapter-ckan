package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.services.DataAssetService;
import de.fraunhofer.fokus.ids.services.FileService;
import de.fraunhofer.fokus.ids.services.ckan.CKANServiceVerticle;
import de.fraunhofer.fokus.ids.services.database.DatabaseServiceVerticle;
import de.fraunhofer.fokus.ids.services.InitService;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.http.entity.ContentType;

import java.util.*;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class MainVerticle extends AbstractVerticle {
    private static Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
    private Router router;
    private DataAssetService dataAssetService;
    private FileService fileService;
    @Override
    public void start(Future<Void> startFuture) {
        this.router = Router.router(vertx);
        this.dataAssetService = new DataAssetService(vertx);
        this.fileService = new FileService(vertx);
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setWorker(true);

        vertx.deployVerticle(DatabaseServiceVerticle.class.getName(), deploymentOptions, reply -> {
                if(reply.succeeded()){
                    LOGGER.info("DataBaseService started");
                    new InitService(vertx, reply2 -> {
                        if(reply2.succeeded()){
                            LOGGER.info("Initialization complete.");
                        }
                        else{
                            LOGGER.error("Initialization failed.", reply2.cause());
                        }
                    });

                }
                else{
                    LOGGER.error("DataBaseService failed", reply.cause());
                }
        });
        vertx.deployVerticle(CKANServiceVerticle.class.getName(), deploymentOptions, reply -> LOGGER.info("CKANService started"));

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

        router.post("/create").handler(routingContext ->
                dataAssetService.createDataAsset(Json.decodeValue(routingContext.getBodyAsJson().toString(), DataAssetCreateMessage.class), reply ->
                        reply(reply, routingContext.response())));

        router.get("/delete/:id").handler(routingContext ->
                dataAssetService.deleteDataAsset(Long.parseLong(routingContext.request().getParam("id")), reply ->
                        reply(reply, routingContext.response())));

        router.post("/getFile").handler(routingContext ->
                fileService.getFile(Json.decodeValue(routingContext.getBodyAsString(), ResourceRequest.class),  routingContext.response()));

        router.route("/supported")
                .handler(routingContext ->
                        supported(result -> reply(result, routingContext.response()))
                );

        router.route("/getDataAssetFormSchema")
                .handler(routingContext ->
                        getDataAssetFormSchema(result -> reply(result, routingContext.response()))
                );

        router.route("/getDataSourceFormSchema")
                .handler(routingContext ->
                        getDataSourceFormSchema(result -> reply(result, routingContext.response()))
                );


        LOGGER.info("Starting CKAN adapter...");
        server.requestHandler(router).listen(8080);
        LOGGER.info("CKAN adapter successfully started.");
    }

    private void supported(Handler<AsyncResult<String>> next) {
        LOGGER.info("Returning supported data formats.");
        JsonArray types = new JsonArray();
        types.add(FileType.JSON);
        types.add(FileType.XML);
        types.add(FileType.TXT);

        JsonObject jO = new JsonObject();
        jO.put("supported", types);

        next.handle(Future.succeededFuture(jO.toString()));
    }

    private void getDataAssetFormSchema(Handler<AsyncResult<String>> next) {
        LOGGER.info("Returning form schema for data asset.");
        JsonObject jO = new JsonObject();
        jO.put("type","object");
        jO.put("properties", new JsonObject()
                .put("resourceId", new JsonObject()
                        .put("type", "string")
                        .put("ui", new JsonObject()
                                .put("label", "Resource ID")
                                .put("placeholder", "27b4920f-e85a-436e-a1a8-e000649abb28"))));
        next.handle(Future.succeededFuture(jO.toString()));
    }

    private void getDataSourceFormSchema(Handler<AsyncResult<String>> next) {
        LOGGER.info("Returning form schema for data source.");

        JsonObject jO = new JsonObject();
        jO.put("type","object");
        jO.put("properties", new JsonObject()
                .put("ckanApiUrl", new JsonObject()
                        .put("type", "string")
                        .put("ui", new JsonObject()
                                .put("label", "CKAN API URL")
                                .put("placeholder", "https://localhost:443/api/3/action"))));

        next.handle(Future.succeededFuture(jO.toString()));
    }

    private void reply(AsyncResult result, HttpServerResponse response) {
        if (result.succeeded()) {
            if (result.result() != null) {
                String entity = result.result().toString();
                response.putHeader("content-type", ContentType.APPLICATION_JSON.toString());
                response.end(entity);
            } else {
                response.setStatusCode(404).end();
            }
        } else {
            response.setStatusCode(404).end();
        }
    }
}