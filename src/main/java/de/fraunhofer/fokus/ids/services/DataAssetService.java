package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.CKANDataset;
import de.fraunhofer.fokus.ids.models.CKANResource;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.stream.Collectors;

public class DataAssetService extends AbstractVerticle {

    final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private EventBus eb;
    private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";
    private WebClient webClient;

    private String RESOURCE_SHOW = "/resource_show?id=";
    private String PACKAGE_SHOW = "/package_show?id=";


    @Override
    public void start(Future<Void> startFuture) {
        webClient = WebClient.create(vertx);
        eb = vertx.eventBus();

        eb.consumer(ROUTE_PREFIX + "ckan.createDataAsset", receivedMessage -> createDataAsset(receivedMessage));

        eb.consumer(ROUTE_PREFIX + "ckan.createDataAsset", receivedMessage -> deleteDataAsset(receivedMessage));
    }

    private void deleteDataAsset(Message<Object> receivedMessage) {


    }


    private void createDataAsset(Message<Object> receivedMessage) {
        DataAssetCreateMessage message = Json.decodeValue(receivedMessage.body().toString(), DataAssetCreateMessage.class);
        final DataAsset dataAsset = new DataAsset();
        dataAsset.setSourceID(message.getDataSource().getId().toString());
        dataAsset.setResourceID(message.getJob().getData().getString("resourceId"));

        buildDataAsset(da ->
                        replyDataAsset(da,
                                receivedMessage),
                dataAsset,
                message.getDataSource());
    }

    private void buildDataAsset(Handler<AsyncResult<DataAsset>> next,
                                DataAsset dataAsset,
                                DataSource dataSource){

                Future<JsonObject> resourceFuture = Future.future();
                queryCKAN(dataSource, dataAsset.getResourceID(), RESOURCE_SHOW, resourceFuture);
                resourceFuture.setHandler(daF -> {
                    if (daF.succeeded()) {
                        CKANResource ckanResource = Json.decodeValue(resourceFuture.result().toString(), CKANResource.class);
                        dataAsset.setFormat(ckanResource.format);
                        dataAsset.setName(ckanResource.name);
                        dataAsset.setResourceID(ckanResource.id);
                        dataAsset.setUrl(ckanResource.url);
                        dataAsset.setOrignalResourceURL(ckanResource.originalURL);
                        dataAsset.setDatasetID(ckanResource.package_id);

                        Future<JsonObject> datasetFuture = Future.future();
                        Future<File> fileFuture = Future.future();
                        downloadResource(fileFuture, dataAsset.getUrl());
                        queryCKAN(dataSource, dataAsset.getDatasetID(), PACKAGE_SHOW, datasetFuture);

                        CompositeFuture.all(datasetFuture, fileFuture).setHandler(ac -> {
                            if (ac.succeeded()) {
                                CKANDataset ckanDataset = Json.decodeValue(datasetFuture.result().toString(), CKANDataset.class);
                                dataAsset.setDatasetNotes(ckanDataset.notes);
                                dataAsset.setDatasetTitle(ckanDataset.title);
                                dataAsset.setLicenseTitle(ckanDataset.license_title);
                                dataAsset.setLicenseUrl(ckanDataset.license_url);
                                dataAsset.setOrignalDatasetURL(ckanDataset.originalURL);
                                dataAsset.setOrganizationDescription(ckanDataset.organization.description);
                                dataAsset.setOrganizationTitle(ckanDataset.organization.title);
                                dataAsset.setTags(ckanDataset.tags.stream().map(t -> t.display_name).collect(Collectors.toList()));
                                dataAsset.setOrganizationDescription(ckanDataset.organization.description);
                                dataAsset.setVersion(ckanDataset.version);
                                dataAsset.setDataSetDescription("");
                                dataAsset.setSignature("");
                                dataAsset.setStatus(DataAssetStatus.APPROVED);

                                File file = fileFuture.result();
                                dataAsset.setAccessInformation(file.getName());
                                next.handle(Future.succeededFuture(dataAsset));

                            }
                            else{
                                LOGGER.error("DataAsset and File Futures could not be completed.\n\n" + daF.cause());
                                next.handle(Future.failedFuture(daF.cause()));
                            }
                        });
                    } else {
                        LOGGER.error("DataAsset Future could not be completed.\n\n" + daF.cause());
                        next.handle(Future.failedFuture(daF.cause()));
                    }
                });
    }

    /**
     *
     * @param result
     * @param receivedMessage
     */
    private void replyDataAsset(AsyncResult<DataAsset> result, Message<Object> receivedMessage){
        if(result.succeeded()) {
            receivedMessage.reply(Json.encode(result.result()));
        }
        else {
            LOGGER.error("Final Data Asset future failed.\n\n"+result.cause());
            receivedMessage.fail(0, "DataAsset could not be created.");
        }
    }

    /**
     *
     * @param future
     * @param urlString
     */
    private void downloadResource(Future future, String urlString) {
        getFile(file ->
                        downloadFile(file,
                                urlString,
                                future),
                urlString);
    }

    /**
     * Method to query the CKAN api of the DataSource
     * @param resourceID ID of the resource to be retrieved from the api
     * @param resourceAPIPath API path to query
     * @param future TODO
     */
    private void queryCKAN(DataSource dataSource, String resourceID, String resourceAPIPath, Future future) {
        LOGGER.info("Querying CKAN.");
        try {
            URL dsUrl = new URL(dataSource.getData().getString("ckanApiUrl"));
            String host = dsUrl.getHost();
            int port = Integer.parseInt(dataSource.getData().getString("ckanPort"));
            String path = dsUrl.getPath() == "/" ? "" : dsUrl.getPath() + resourceAPIPath + resourceID;

            webClient
                    .get(port, host, path)
                    .send(ar -> {
                        if (ar.succeeded()) {
                            future.complete(ar.result().bodyAsJsonObject().getJsonObject("result").put("originalURL", host + path));
                        } else {
                            LOGGER.error("No response from CKAN.\n\n" + ar.cause().getMessage());
                            future.fail(ar.cause().getMessage());
                        }
                    });
        } catch (MalformedURLException e) {
            LOGGER.error(e);
            future.fail(e.getMessage());
        }
    }

    /**
     * Method to initiate creation of a file for later download
     * @param next Handler to download file content into the created file
     * @param urlString TODO
     */
    private void getFile(Handler<AsyncResult<File>> next, String urlString){
        LOGGER.info("Starting to query file information.");
        URL url;
        try {
            url = new URL(urlString);
            eb.send(ROUTE_PREFIX+"ckan.repositoryService.createFile", Json.encode(url), res -> {
                if(res.succeeded()) {
                    try {
                        LOGGER.info("File is created on the file system.");
                        next.handle(Future.succeededFuture(Json.decodeValue(res.result().body().toString(), File.class)));
                    } catch (Exception e) {
                        LOGGER.error("File could not be found via repository service.", e);
                        next.handle(Future.failedFuture(res.cause()));
                    }
                }
                else {
                    LOGGER.error("File could not be created.\n\n"+res.cause().toString());
                    next.handle(Future.failedFuture(res.cause()));
                }
            });
        }
        catch (MalformedURLException e){
            LOGGER.error("URL could not be resolved",e);
            next.handle(Future.failedFuture(e.getMessage()));
        }
    }

    /**
     * Method to perform download of the resource file
     * @param result File Future produced by getFile()
     * @param urlString TODO
     * @param future TODO
     */
    private void downloadFile(AsyncResult<File> result, String urlString, Future future){
        if(result.failed()) {
            LOGGER.error("File Future could not be completed.\n\n"+result.cause());
            future.fail(result.cause().toString());
        }else {
            LOGGER.info("Starting to download DataAsset file.");
            URL url;
            try {
                url = new URL(urlString);
                final int port = url.getPort() == -1 ? 80 : url.getPort();
                final String host = url.getHost();
                final String path = url.getPath();

                File file = result.result();
                vertx.fileSystem().open(file.getAbsolutePath(),
                        new OpenOptions().setWrite(true).setCreate(true),
                        fres -> {
                            if (fres.succeeded()) {
                                webClient
                                        .get(port, host, path)
                                        .as(BodyCodec.pipe(fres.result()))
                                        .send(ar -> {
                                            if (ar.succeeded()) {
                                                HttpResponse<Void> response = ar.result();
                                                LOGGER.info("Received response with status code " + response.statusCode() + ". File is downloaded.");
                                                future.complete(file);
                                            } else {
                                                LOGGER.error("File could not be downloaded.\n\n" + ar.cause());
                                                future.fail(ar.cause().toString());
                                            }
                                        });
                            } else {
                                LOGGER.error("Filesystem could not be accessed.\n\n" + fres.cause().toString());
                                future.fail(fres.cause().toString());
                            }
                        });
            } catch (MalformedURLException e) {
                LOGGER.error("URL could not be resolved",e);
                future.fail(e.getMessage());
            }
        }
    }
}
