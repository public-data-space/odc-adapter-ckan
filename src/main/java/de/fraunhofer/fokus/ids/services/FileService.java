package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import de.fraunhofer.fokus.ids.services.repository.RepositoryService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class FileService {

    final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private RepositoryService repositoryService;
    private DatabaseService databaseService;

    public FileService(Vertx vertx){
        this.repositoryService = RepositoryService.createProxy(vertx, Constants.REPOSITORY_SERVICE);
        this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void getFile(ResourceRequest resourceRequest, Handler<AsyncResult<JsonObject>> resultHandler) {

        if(resourceRequest.getFileType().equals(FileType.JSON)) {
            getPayload(resourceRequest.getDataAsset(), "json", resultHandler);
        }
        if(resourceRequest.getFileType().equals(FileType.TXT)) {
            getPayload(resourceRequest.getDataAsset(), "txt", resultHandler);
        }
        getPayload(resourceRequest.getDataAsset(), "multi", resultHandler);
    }

    private void getPayload(DataAsset dataAsset, String extension, Handler<AsyncResult<JsonObject>> resultHandler) {

        getAccessInformation(fileName ->
            getFileContent( fileContent ->
                    transform( transformedFileContent ->
                            replyFile( transformedFileContent,
                                    resultHandler),
                            fileContent,
                            extension),
                    fileName
                    ),
                dataAsset);
    }

    private void getAccessInformation(Handler<AsyncResult<String>> resultHandler, DataAsset dataAsset){

        databaseService.query("SELECT filename from accessinformation WHERE dataassetid = ?", new JsonArray().add(dataAsset.getId()), reply -> {

            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture(reply.result().get(0).getString("filename")));
            }
            else{
                LOGGER.info("File information could not be retrieved.\n\n"+reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });

    }

    private void getFileContent(Handler<AsyncResult<String>> next, AsyncResult<String> fileName){
        if(fileName.succeeded()) {
            repositoryService.getContent(fileName.result(), reply -> {
                if (reply.succeeded()) {
                    next.handle(Future.succeededFuture(reply.result()));
                } else {
                    LOGGER.error("FileContent could not be read.\n\n" + reply.cause());
                    next.handle(Future.failedFuture(reply.cause()));
                }
            });
        }
        else{
            next.handle(Future.failedFuture(fileName.cause()));
        }
    }

    private void transform(Handler<AsyncResult<String>> next, AsyncResult<String> result, String fileType){
        if(fileType.equals("json")) {
        //TODO File transformation magic?
        } else {
        //TODO File transformation magic?
        }
        next.handle(Future.succeededFuture(result.result()));
    }

    private void replyFile(AsyncResult<String> result, Handler<AsyncResult<JsonObject>> resultHandler){
        if (result.succeeded()) {
            JsonObject jO = new JsonObject();
            jO.put("result", result.result());
            resultHandler.handle(Future.succeededFuture(jO));
        }
        else {
            LOGGER.error("FileContent could not be read.\n\n"+result.cause());
            resultHandler.handle(Future.failedFuture(result.cause()));
        }
    }
}
