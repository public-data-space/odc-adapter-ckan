package de.fraunhofer.fokus.ids.services.ckan;

import de.fraunhofer.fokus.ids.persistence.entities.DataSource;
import de.fraunhofer.fokus.ids.persistence.entities.serialization.DataSourceSerializer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class CKANServiceImpl implements CKANService {
    private final Logger LOGGER = LoggerFactory.getLogger(CKANServiceImpl.class.getName());
    private WebClient webClient;

    public CKANServiceImpl(WebClient webClient, Handler<AsyncResult<CKANService>> readyHandler) {
        this.webClient = webClient;
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public CKANService query(JsonObject dataSourceJson, String resourceID, String resourceAPIPath, Handler<AsyncResult<JsonObject>> resultHandler) {
        LOGGER.info("Querying CKAN.");
        DataSource dataSource = null;
        try {
            dataSource = DataSourceSerializer.deserialize(dataSourceJson);
        } catch (ParseException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e));
        }
        try {
            URL dsUrl = new URL(dataSource.getData().getString("ckanApiUrl"));
            String host = dsUrl.getHost();
            int port = Integer.parseInt(dataSource.getData().getString("ckanPort"));
            String path = dsUrl.getPath().equals("/") ? "" : dsUrl.getPath() + resourceAPIPath + resourceID;

            webClient
                    .get(port, host, path)
                    .send(ar -> {
                        if (ar.succeeded()) {
                            resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject().getJsonObject("result").put("originalURL", host + path)));
                        } else {
                            LOGGER.error("No response from CKAN.", ar.cause());
                            resultHandler.handle(Future.failedFuture(ar.cause()));
                        }
                    });
        } catch (MalformedURLException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e.getMessage()));
        }
        return this;
    }
}
