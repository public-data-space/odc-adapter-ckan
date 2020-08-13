package de.fraunhofer.fokus.ids.services.ckan;

import de.fraunhofer.fokus.ids.services.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.serviceproxy.ServiceBinder;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class CKANServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        WebClientOptions options = new WebClientOptions().setTrustAll(true);
        WebClient webClient = WebClient.create(vertx, options);
        CKANService.create(webClient, ready -> {
            if (ready.succeeded()) {
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                        .setAddress(Constants.CKAN_SERVICE)
                        .register(CKANService.class, ready.result());
                startPromise.complete();
            } else {
                startPromise.fail(ready.cause());
            }
        });
    }

}
