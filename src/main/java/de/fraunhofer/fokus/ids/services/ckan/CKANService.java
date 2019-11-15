package de.fraunhofer.fokus.ids.services.ckan;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
@ProxyGen
@VertxGen
public interface CKANService {

    @Fluent
    CKANService query(JsonObject dataSourceJson, String resourceID, String resourceAPIPath, Handler<AsyncResult<JsonObject>> resultHandler);

    @GenIgnore
    static CKANService create(WebClient webClient, Handler<AsyncResult<CKANService>> readyHandler) {
        return new CKANServiceImpl(webClient, readyHandler);
    }

    @GenIgnore
    static CKANService createProxy(Vertx vertx, String address) {
        return new CKANServiceVertxEBProxy(vertx, address);
    }

}
