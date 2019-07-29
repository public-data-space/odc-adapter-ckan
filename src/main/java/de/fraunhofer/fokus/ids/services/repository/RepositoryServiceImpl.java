package de.fraunhofer.fokus.ids.services.repository;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class RepositoryServiceImpl implements RepositoryService {

    final Logger LOGGER = LoggerFactory.getLogger(RepositoryServiceImpl.class.getName());

    private String repoPath;
    private Vertx vertx;
    private WebClient webClient;

    public RepositoryServiceImpl(WebClient webClient, Vertx vertx, Handler<AsyncResult<RepositoryService>> readyHandler) {
        getConfig(vertx);
        this.vertx = vertx;
        this.webClient = webClient;
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public RepositoryService getContent(String fileName, Handler<AsyncResult<String>> resultHandler) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(this.repoPath + "/" + fileName)));
            resultHandler.handle(Future.succeededFuture(content));
        } catch (IOException e) {
            LOGGER.info("",e);
            resultHandler.handle(Future.failedFuture(e.getMessage()));
        }
        return this;
    }

    @Override
    public RepositoryService deleteFile(String fileName, Handler<AsyncResult<Void>> resultHandler) {
        File file = new File(this.repoPath + "/" + fileName);
        file.delete();
        resultHandler.handle(Future.succeededFuture());
        return this;
    }

    @Override
    public RepositoryService createFile(String urlString, Handler<AsyncResult<String>> resultHandler) {
        try {
            URL url = new URL(urlString);
            String ext = FilenameUtils.getExtension(url.getFile());
            String id = UUID.randomUUID().toString();
            int qIndex = ext.indexOf("?");
            if(qIndex != -1) {
                ext = ext.substring(0, ext.indexOf("?"));
            }
            String path = this.repoPath + "/" + id + "." + ext;
            File file = new File(path);
            resultHandler.handle(Future.succeededFuture(file.getAbsolutePath()));
        } catch (MalformedURLException e) {
            LOGGER.info("URL could not be decoded.\n\n",e);
            resultHandler.handle(Future.failedFuture(e.getMessage()));
        }
        return this;
    }

//    public String getFileContent(DataAsset dataAsset) {
//        String name = dataAsset.getAccessInformation();
//        String content = null;
//        try {
//            content = new String(Files.readAllBytes(Paths.get(this.repoPath + "/" + name)));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return content;
//    }

//    public File getFile(String name) {
//        File file = new File(this.repoPath + "/" + name);
//        if(file.exists()) {
//            return file;
//        } else {
//            return null;
//        }
//    }

    @Override
    public RepositoryService downloadResource(String urlString, Handler<AsyncResult<String>> resultHandler) {
        getFile(file ->
                        downloadFile(file,
                                urlString,
                                resultHandler),
                urlString);

        return this;
    }

    /**
     * Method to initiate creation of a file for later download
     * @param next Handler to download file content into the created file
     * @param urlString TODO
     */
    private void getFile(Handler<AsyncResult<String>> next, String urlString){
        LOGGER.info("Starting to query file information.");
        createFile(urlString, res -> {
                if(res.succeeded()) {
                    try {
                        LOGGER.info("File is created on the file system.");
                        next.handle(Future.succeededFuture(res.result()));
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

    /**
     * Method to perform download of the resource file
     * @param result File Future produced by getFile()
     * @param urlString TODO
     */
    private void downloadFile(AsyncResult<String> result, String urlString, Handler<AsyncResult<String>> resultHandler){
        if(result.failed()) {
            LOGGER.error("File Future could not be completed.\n\n"+result.cause());
            resultHandler.handle(Future.failedFuture(result.cause()));
        }else {
            LOGGER.info("Starting to download DataAsset file.");
            URL url;
            try {
                url = new URL(urlString);
                final int port = url.getPort() == -1 ? 80 : url.getPort();
                final String host = url.getHost();
                final String path = url.getPath();

                File file = new File(result.result());
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
                                                resultHandler.handle(Future.succeededFuture(file.getName()));
                                            } else {
                                                LOGGER.error("File could not be downloaded.\n\n" + ar.cause());
                                                resultHandler.handle(Future.failedFuture(ar.cause()));
                                            }
                                        });
                            } else {
                                LOGGER.error("Filesystem could not be accessed.\n\n" + fres.cause().toString());
                                resultHandler.handle(Future.failedFuture(fres.cause()));
                            }
                        });
            } catch (MalformedURLException e) {
                LOGGER.error("URL could not be resolved",e);
                resultHandler.handle(Future.failedFuture(e.getMessage()));
            }
        }
    }

    private void getConfig(Vertx vertx) {
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setConfig(new JsonObject().put("path", this.getClass().getClassLoader().getResource("conf/application.conf").getFile()));

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                this.repoPath = ar.result().getJsonObject("config").getJsonObject("repository").getString("path");
            } else {
                LOGGER.error("Config could not be retrieved.");
            }
        });
    }

}
