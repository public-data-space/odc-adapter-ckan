package de.fraunhofer.fokus.ids.services.repository;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
@ProxyGen
@VertxGen
public interface RepositoryService {

    @Fluent
    RepositoryService getContent(String fileName, Handler<AsyncResult<String>> resultHandler);

    @Fluent
    RepositoryService deleteFile(String fileName, Handler<AsyncResult<Void>> resultHandler);

    @Fluent
    RepositoryService createFile(String urlString, Handler<AsyncResult<String>> resultHandler);

    @Fluent
    RepositoryService downloadResource(String urlString, Handler<AsyncResult<String>> resultHandler);

    @GenIgnore
    static RepositoryService create(WebClient webClient, Vertx vertx, Handler<AsyncResult<RepositoryService>> readyHandler) {
        return new RepositoryServiceImpl(webClient, vertx, readyHandler);
    }

    @GenIgnore
    static RepositoryService createProxy(Vertx vertx, String address) {
        return new RepositoryServiceVertxEBProxy(vertx, address);
    }
}