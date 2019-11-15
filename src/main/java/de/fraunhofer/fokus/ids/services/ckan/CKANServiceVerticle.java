package de.fraunhofer.fokus.ids.services.ckan;

import de.fraunhofer.fokus.ids.services.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class CKANServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        WebClient webClient = WebClient.create(vertx);
        CKANService.create(webClient, ready -> {
            if (ready.succeeded()) {
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                        .setAddress(Constants.CKAN_SERVICE)
                        .register(CKANService.class, ready.result());
                startFuture.complete();
            } else {
                startFuture.fail(ready.cause());
            }
        });
    }

}
