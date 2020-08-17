package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class FileService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private DatabaseService databaseService;
    WebClient webClient;
    public FileService(Vertx vertx){
        WebClientOptions options = new WebClientOptions().setTrustAll(true);
        this.webClient = WebClient.create(vertx, options);
        this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void getFile(ResourceRequest resourceRequest,HttpServerResponse httpServerResponse) {

        if(resourceRequest.getFileType().equals(FileType.JSON)) {
            getPayload(resourceRequest.getDataAsset(), "json",httpServerResponse);
        }
        if(resourceRequest.getFileType().equals(FileType.TXT)) {
            getPayload(resourceRequest.getDataAsset(), "txt",httpServerResponse);
        }
        getPayload(resourceRequest.getDataAsset(), "multi",httpServerResponse);
    }

    private void getPayload(DataAsset dataAsset, String extension,HttpServerResponse httpServerResponse) {
        getAccessInformation(resultHandler->{
            if (resultHandler.succeeded()){
                if (resultHandler.result() != null) {
                    streamFile(resultHandler.result(),httpServerResponse);
                } else {
                    LOGGER.error("File is null");
                    httpServerResponse.setStatusCode(404).end();
                }
            }else {
                LOGGER.error(resultHandler.cause());
                httpServerResponse.setStatusCode(404).end();
            }},dataAsset);
    }

    private void getAccessInformation(Handler<AsyncResult<String>> resultHandler, DataAsset dataAsset){

        databaseService.query("SELECT filename from accessinformation WHERE dataassetid = ?", new JsonArray().add(dataAsset.getId()), reply -> {

            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture(reply.result().get(0).getString("filename")));
            }
            else{
                LOGGER.error("File information could not be retrieved.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });

    }

    public void streamFile(String urlString , HttpServerResponse response){
        URL url = null;
        try {
            url = new URL(urlString);
        } catch (MalformedURLException e) {
            LOGGER.error(e);
            response.setStatusCode(404).end();
        }
        LOGGER.info("Piping file from "+urlString);
        response.putHeader("Transfer-Encoding", "chunked");
        webClient
                .getAbs(url.toString())
                .as(BodyCodec.pipe(response))
                .send(ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Void> response2 = ar.result();
                        LOGGER.info("Received response with status code " + response2.statusCode());
                    } else {
                       LOGGER.error("Something went wrong " + ar.cause().getMessage());
                    }
                });
    }

    private void transform(Handler<AsyncResult<String>> next, AsyncResult<String> result, String fileType){
        if(fileType.equals("json")) {
        //TODO File transformation magic?
        } else {
        //TODO File transformation magic?
        }
        next.handle(Future.succeededFuture(result.result()));
    }

    private void replyFile(AsyncResult<String> result, Handler<AsyncResult<String>> resultHandler){
        if (result.succeeded()) {
            resultHandler.handle(Future.succeededFuture(result.result()));
        }
        else {
            LOGGER.error("FileContent could not be read.", result.cause());
            resultHandler.handle(Future.failedFuture(result.cause()));
        }
    }
}
