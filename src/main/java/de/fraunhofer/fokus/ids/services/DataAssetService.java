package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.messages.DataAssetCreateMessage;
import de.fraunhofer.fokus.ids.models.CKANDataset;
import de.fraunhofer.fokus.ids.models.CKANResource;
import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.Dataset;
import de.fraunhofer.fokus.ids.persistence.entities.Distribution;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;
import de.fraunhofer.fokus.ids.services.ckan.CKANService;
import de.fraunhofer.fokus.ids.services.database.DatabaseService;
import io.vertx.core.*;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FilenameUtils;

import java.net.URL;
import java.text.DateFormat;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataAssetService.class.getName());

    private CKANService ckanService;
    private DatabaseService databaseService;
    private FileService fileService;

    private String RESOURCE_SHOW = "/resource_show?id=";
    private String PACKAGE_SHOW = "/package_show?id=";

    public DataAssetService(Vertx vertx){
        this.ckanService = CKANService.createProxy(vertx, Constants.CKAN_SERVICE);
        this.databaseService = DatabaseService.createProxy(vertx, Constants.DATABASE_SERVICE);
        this.fileService = new FileService(vertx);
    }

    public void deleteDataAsset(Long id, Handler<AsyncResult<JsonObject>> resultHandler) {
        getFilename(id, fileNameReply -> {
            if(fileNameReply.succeeded()){
                        databaseService.update("DELETE FROM accessinformation WHERE dataassetid=?", new JsonArray().add(id), databaseDeleteReply -> {
                            if(databaseDeleteReply.succeeded()) {
                                LOGGER.info("Data Asset successfully deleted.");
                                resultHandler.handle(Future.succeededFuture(new JsonObject().put("status","success")));
                            }
                            else{
                                LOGGER.error("Data Asset could not be deleted.");
                                resultHandler.handle(Future.failedFuture(databaseDeleteReply.cause()));
                            }
                        });
            } else {
                LOGGER.error(fileNameReply.cause());
                resultHandler.handle(Future.failedFuture(fileNameReply.cause()));
            }
        });
    }

    private void getFilename(long id, Handler<AsyncResult<String>> resultHandler){
        databaseService.query("SELECT filename from accessinformation WHERE dataassetid = ?", new JsonArray().add(id), reply -> {

            if(reply.succeeded()){
                resultHandler.handle(Future.succeededFuture(reply.result().get(0).getString("filename")));
            }
            else{
                LOGGER.error("File information could not be retrieved.", reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }


    private void saveAccessInformation(AsyncResult<DataAsset> dataAsset, Handler<AsyncResult<DataAsset>> next){
        if(dataAsset.succeeded()){
            DataAsset dataAsset1 = dataAsset.result();
            Date d = new Date();
            databaseService.update("INSERT INTO accessinformation values(?,?,?,?)",
                    new JsonArray().add(d.toInstant()).add(d.toInstant())
                            .add(dataAsset1.getId())
                            .add(dataAsset1.getUrl()), reply2 -> {
                        if(reply2.succeeded()){
                            next.handle(Future.succeededFuture(dataAsset1));
                        }
                        else{
                            LOGGER.error("Access information could not be inserted into database.", reply2.cause());
                            next.handle(Future.failedFuture(reply2.cause()));
                        }
                    });
        }
        else{
            next.handle(Future.failedFuture(dataAsset.cause()));
        }
    }

    public void createDataAsset(DataAssetCreateMessage message, Handler<AsyncResult<JsonObject>> resultHandler) {
        final Dataset dataset = new Dataset();
        dataset.setSourceid(message.getDataSource().getId());
        dataset.setId(message.getDataAssetId());
        buildDataAsset(da ->
                        saveAccessInformation(da, v ->
                                replyDataAsset(v,
                                        resultHandler)),
                dataset,
                message.getData().getString("resourceId",""),
                message.getDataSource());
    }

    private void buildDataAsset(Handler<AsyncResult<Dataset>> next,
                                Dataset dataAsset,
                                String id,
                                DataSource dataSource){

        ckanService.query(new JsonObject(Json.encode(dataSource)), id, PACKAGE_SHOW, packageReply -> {
            if (packageReply.succeeded()) {
                if(!packageReply.result().getBoolean("success")){
                    handleResourceURI(next, dataAsset, id, dataSource);
                } else {
                    handlePackageURI(next, dataAsset, id, dataSource);
                }
            }
        });






            ckanService.query(new JsonObject(Json.encode(dataSource)), dataAsset.getResourceID(), RESOURCE_SHOW, reply -> {
                if (reply.succeeded()) {
                    CKANResource ckanResource = Json.decodeValue(reply.result().toString(), CKANResource.class);
                    dataAsset.setFormat(ckanResource.format);
                    dataAsset.setName(ckanResource.name);
                    dataAsset.setResourceID(ckanResource.id);
                    dataAsset.setUrl(ckanResource.url);
                    dataAsset.setOrignalResourceURL(ckanResource.originalURL);
                    dataAsset.setDatasetID(ckanResource.package_id);

                    ckanService.query(new JsonObject(Json.encode(dataSource)), dataAsset.getDatasetID(), PACKAGE_SHOW, reply2 -> {
                            if (reply2.succeeded()) {
                                CKANDataset ckanDataset = Json.decodeValue(reply2.result().toString(), CKANDataset.class);
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
                                dataAsset.setDataSetDescription(ckanDataset.notes);
                                dataAsset.setSignature("");
                                dataAsset.setStatus(DataAssetStatus.APPROVED);

                                try {
                                    String filename = Paths.get(new URI(ckanResource.url).getPath()).getFileName().toString();
                                    if(ckanResource.format != null && filename.toLowerCase().endsWith(ckanResource.format.toLowerCase())) {
                                        dataAsset.setFilename(filename);
                                    } else {
                                        dataAsset.setFilename(filename+"."+ckanResource.format);
                                    }
                                } catch (URISyntaxException e) {
                                    LOGGER.info("Filename could not be extracted from URL.");
                                    if(ckanResource.format != null){
                                        dataAsset.setFilename(UUID.randomUUID().toString()+"."+ckanResource.format);
                                    } else {
                                        dataAsset.setFilename(UUID.randomUUID().toString());
                                    }
                                }

                                next.handle(Future.succeededFuture(dataAsset));

                            } else {
                                LOGGER.error("DataAsset and File Futures could not be completed.", reply2.cause());
                                next.handle(Future.failedFuture(reply2.cause()));
                            }
                        });
                } else {
                    LOGGER.error("DataAsset Future could not be completed.", reply.cause());
                    next.handle(Future.failedFuture(reply.cause()));
                }
        });
    }

    private void handlePackageURI(Handler<AsyncResult<Dataset>> next,
                                  Dataset dataAsset,
                                  String id,
                                  DataSource dataSource) {

        queryPackage(id, dataSource, reply -> {
            if(reply.succeeded()){
                CKANDataset ckanDataset = reply.result();
                dataAsset.setTags(ckanDataset.tags.stream().map(t -> t.display_name).collect(Collectors.toSet()));
                dataAsset.setVersion(ckanDataset.version);
                dataAsset.setStatus(DataAssetStatus.APPROVED);
                dataAsset.setDescription(ckanDataset.notes);
                dataAsset.setTitle(ckanDataset.title);
                dataAsset.setLicense(ckanDataset.license_url);
                Set<Distribution> distributions = new HashSet();
                for(CKANResource cr : ckanDataset.resources){
                    if(cr.id.equals(id)){
                        Distribution distribution = new Distribution();
                        distribution.setFilename(FilenameUtils.getName(cr.url));
                        distribution.setTitle(cr.name);
                        distribution.setDescription(cr.description);
                        distribution.setFiletype(resolveFormat(cr.format));
                        distributions.add(distribution);
                    }
                }
                dataAsset.setDistributions(distributions);
            }

        });

    }

    private void handleResourceURI(Handler<AsyncResult<Dataset>> next,
                                   Dataset dataAsset,
                                   String id,
                                   DataSource dataSource) {

        queryPackage(id, dataSource, reply -> {
            if(reply.succeeded()){
                CKANDataset ckanDataset = reply.result();
                dataAsset.setTags(ckanDataset.tags.stream().map(t -> t.display_name).collect(Collectors.toSet()));
                dataAsset.setVersion(ckanDataset.version);
                dataAsset.setStatus(DataAssetStatus.APPROVED);
                dataAsset.setDescription(ckanDataset.notes);
                dataAsset.setTitle(ckanDataset.title);
                dataAsset.setLicense(ckanDataset.license_url);
                for(CKANResource cr : ckanDataset.resources){
                    if(cr.id.equals(id)){
                        Distribution distribution = new Distribution();
                        distribution.setFilename(FilenameUtils.getName(cr.url));
                        distribution.setTitle(cr.name);
                        distribution.setDescription(cr.description);
                        distribution.setFiletype(resolveFormat(cr.format));
                        Set<Distribution> distributions = new HashSet();
                        distributions.add(distribution);
                        dataAsset.setDistributions(distributions);
                    }
                }
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

    private void replyDataAsset(AsyncResult<DataAsset> result, Handler<AsyncResult<JsonObject>> resultHandler){
        if(result.succeeded()) {
            resultHandler.handle(Future.succeededFuture(new JsonObject(Json.encode(result.result()))));
        }
        else {
            LOGGER.error("Final Data Asset future failed.", result.cause());
            resultHandler.handle(Future.failedFuture(result.cause()));
        }
    }
}
