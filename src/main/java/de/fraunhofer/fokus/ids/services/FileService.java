package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.enums.FileType;
import de.fraunhofer.fokus.ids.messages.ResourceRequest;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class FileService extends AbstractVerticle {

    final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private EventBus eb;
    private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";

    @Override
    public void start(Future<Void> startFuture) {
        eb = vertx.eventBus();
        eb.consumer(ROUTE_PREFIX + "ckan.getFile", receivedMessage -> payload(receivedMessage));
    }

    public void payload(Message<Object> receivedMessage) {

        ResourceRequest request = Json.decodeValue(receivedMessage.body().toString(), ResourceRequest.class);

        if(request.getFileType().equals(FileType.JSON)) {
            getPayload(request.getDataAsset(), "json", receivedMessage);
        }
        if(request.getFileType().equals(FileType.TXT)) {
            getPayload(request.getDataAsset(), "txt", receivedMessage);
        }
        getPayload(request.getDataAsset(), "multi", receivedMessage);
    }

    private void getPayload(DataAsset dataAsset, String extension, Message<Object> receivedMessage) {

        getFileContent( fileContent ->
                transform( transformedFileContent ->
                        replyFile( transformedFileContent,
                                receivedMessage),
                        fileContent,
                        extension),
                dataAsset);
    }

    private void getFileContent(Handler<AsyncResult<String>> next, DataAsset dataAsset){
        eb.send(ROUTE_PREFIX+"ckan.repositoryService.getContent", Json.encode(dataAsset), reply -> {
            if (reply.succeeded()) {
                next.handle(Future.succeededFuture(reply.result().body().toString()));
            }
            else {
                LOGGER.error("FileContent could not be read.\n\n"+reply.cause());
                next.handle(Future.failedFuture(reply.cause()));
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

    private void replyFile(AsyncResult<String> result, Message<Object> receivedMessage){
        if (result.succeeded()) {
            receivedMessage.reply(Json.encode(result.result()));
        }
        else {
            LOGGER.error("FileContent could not be read.\n\n"+result.cause());
            receivedMessage.fail(0,result.cause().toString());
        }
    }
}
