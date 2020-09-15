package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.CKANDataset;
import de.fraunhofer.fokus.ids.models.CKANResource;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.services.ckan.CKANService;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private CKANService ckanService;
    private DatabaseService databaseService;

    private String RESOURCE_SHOW = "/resource_show?id=";
    private String PACKAGE_SHOW = "/package_show?id=";

    public DataAssetService(Vertx vertx){
        this.ckanService = CKANService.createProxy(vertx, Constants.CKAN_SERVICE);
        this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
    }

    public void deleteDataAsset(String id, Handler<AsyncResult<JsonObject>> resultHandler) {
        databaseService.update("DELETE FROM accessinformation WHERE datasetid=?", new JsonArray().add(id), databaseDeleteReply -> {
            if(databaseDeleteReply.succeeded()) {
                LOGGER.info("Data Asset successfully deleted.");
                resultHandler.handle(Future.succeededFuture(new JsonObject().put("status","success")));
            }
            else{
                LOGGER.error("Data Asset could not be deleted.");
                resultHandler.handle(Future.failedFuture(databaseDeleteReply.cause()));
            }
        });
    }

    private void saveAccessInformation(Distribution dist, CKANResource ckanResource, String datasetId, Handler<AsyncResult<Void>> resultHandler){
            Date d = new Date();
            databaseService.update("INSERT INTO accessinformation values(?,?,?,?,?)",
                        new JsonArray().add(d.toInstant()).add(d.toInstant())
                                .add(dist.getResourceId())
                                .add(datasetId)
                                .add(ckanResource.url), reply -> {
                        if (reply.succeeded()) {
                            resultHandler.handle(Future.succeededFuture());
                        } else {
                            LOGGER.error("Access information could not be inserted into database.", reply.cause());
                            resultHandler.handle(Future.failedFuture(reply.cause()));
                        }
                    });
    }

    public void createDataAsset(DataAssetCreateMessage message, Handler<AsyncResult<JsonObject>> resultHandler) {
        final Dataset dataset = new Dataset();
        dataset.setSourceId(message.getDataSource().getId());
        dataset.setResourceId(UUID.randomUUID().toString());
        buildDataAsset(da -> replyDataAsset(da,
                resultHandler),
                message.getData().getString("resourceId",""),
                message.getDataSource());
    }

    private void buildDataAsset(Handler<AsyncResult<Dataset>> next,
                                String id,
                                DataSource dataSource){

        ckanService.query(new JsonObject(Json.encode(dataSource)), id, PACKAGE_SHOW, packageReply -> {
            if (packageReply.succeeded()) {
                handlePackageURI(next, id, dataSource);
            } else {
                handleResourceURI(next, id, dataSource);
            }

        });
    }

    private void handlePackageURI(Handler<AsyncResult<Dataset>> next,
                                  String id,
                                  DataSource dataSource) {

        queryPackage(id, dataSource, packageReply -> {
            if(packageReply.succeeded()){
                CKANDataset ckanDataset = packageReply.result();
                Dataset dataset = buildDataset(ckanDataset);
                List<Promise<Distribution>> promises = new ArrayList<>();
                for(CKANResource cr : ckanDataset.resources){
                    Promise p = Promise.promise();
                    promises.add(p);
                    buildDistribution(cr, dataset, p);
                }
                CompositeFuture.all(promises.stream().map(Promise::future).collect(Collectors.toList())).onComplete(handler -> {
                    if(handler.succeeded()){
                        Set<Distribution> distributions = new HashSet();
                        for(Promise<Distribution> promise : promises){
                            distributions.add(promise.future().result());
                        }
                        dataset.setDistributions(distributions);
                        next.handle(Future.succeededFuture(dataset));
                    } else {
                        LOGGER.error(handler.cause());
                        next.handle(Future.failedFuture(handler.cause()));
                    }
                });
            } else {
                LOGGER.error(packageReply.cause());
                next.handle(Future.failedFuture(packageReply.cause()));
            }

        });

    }

    private void handleResourceURI(Handler<AsyncResult<Dataset>> next,
                                   String id,
                                   DataSource dataSource) {

        queryResource(id, dataSource, resourceReply -> {
            if(resourceReply.succeeded()){
                queryPackage(resourceReply.result().package_id, dataSource, packageReply -> {
                    if(packageReply.succeeded()) {
                        Dataset dataset = buildDataset(packageReply.result());
                        for (CKANResource cr : packageReply.result().resources) {
                            if (cr.id.equals(id)) {
                                buildDistribution(cr, dataset, reply -> {
                                    if(reply.succeeded()){
                                        Set<Distribution> distributions = new HashSet();
                                        distributions.add(reply.result());
                                        dataset.setDistributions(distributions);
                                        next.handle(Future.succeededFuture(dataset));
                                    } else {
                                        LOGGER.error(reply.cause());
                                        next.handle(Future.failedFuture(reply.cause()));
                                    }
                                });
                            }
                        }
                    } else {
                        LOGGER.error(packageReply.cause());
                        next.handle(Future.failedFuture(packageReply.cause()));
                    }
                });
            } else {
                LOGGER.error(resourceReply.cause());
                next.handle(Future.failedFuture(resourceReply.cause()));
            }

        });

    }

    private Dataset buildDataset(CKANDataset ckanDataset){
        Dataset dataset = new Dataset();

        dataset.setTags(ckanDataset.tags.stream().map(t -> t.display_name).collect(Collectors.toSet()));
        dataset.setVersion(ckanDataset.version);
        dataset.setStatus(DataAssetStatus.APPROVED);
        dataset.setDescription(ckanDataset.notes);
        dataset.setTitle(ckanDataset.title);
        dataset.setResourceId(UUID.randomUUID().toString());
        dataset.setLicense(ckanDataset.license_url);
        return dataset;
    }

    private void buildDistribution(CKANResource ckanResource, Dataset dataset, Handler<AsyncResult<Distribution>> resultHandler){
        Distribution distribution = new Distribution();
        distribution.setTitle(ckanResource.name);
        distribution.setDescription(ckanResource.description);
        distribution.setFiletype(ckanResource.format);
        distribution.setResourceId(UUID.randomUUID().toString());
        distribution.setLicense(dataset.getLicense());
        try {
            String filename = Paths.get(new URI(ckanResource.url).getPath()).getFileName().toString();
            if(ckanResource.format != null) {
                distribution.setFilename(filename);
            } else {
                distribution.setFilename(UUID.randomUUID().toString());
            }
        } catch (URISyntaxException e) {
            LOGGER.info("Filename could not be extracted from URL.");
            distribution.setFilename(UUID.randomUUID().toString());
        }
        saveAccessInformation(distribution, ckanResource, dataset.getResourceId(), reply -> {
            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture(distribution));
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void queryPackage(String id, DataSource dataSource, Handler<AsyncResult<CKANDataset>> next) {
        ckanService.query(new JsonObject(Json.encode(dataSource)), id, PACKAGE_SHOW, reply -> {
            if(reply.succeeded()){
                next.handle(Future.succeededFuture(Json.decodeValue(reply.result().toString(), CKANDataset.class)));
            } else {
                next.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void queryResource(String id, DataSource dataSource, Handler<AsyncResult<CKANResource>> next) {
        ckanService.query(new JsonObject(Json.encode(dataSource)), id, RESOURCE_SHOW, reply -> {
            if(reply.succeeded()){
                next.handle(Future.succeededFuture(Json.decodeValue(reply.result().toString(), CKANResource.class)));
            } else {
                next.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void replyDataAsset(AsyncResult<Dataset> result, Handler<AsyncResult<JsonObject>> resultHandler){
        if(result.succeeded()) {
            resultHandler.handle(Future.succeededFuture(new JsonObject(Json.encode(result.result()))));
        }
        else {
            LOGGER.error("Final Data Asset future failed.", result.cause());
            resultHandler.handle(Future.failedFuture(result.cause()));
        }
    }
}
