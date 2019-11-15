package de.fraunhofer.fokus.ids.services.repository;

import de.fraunhofer.fokus.ids.services.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;
/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class RepositoryServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        WebClient webClient = WebClient.create(vertx);
        RepositoryService.create(webClient, vertx, ready -> {
            if (ready.succeeded()) {
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                        .setAddress(Constants.REPOSITORY_SERVICE)
                        .register(RepositoryService.class, ready.result());
                startFuture.complete();
            } else {
                startFuture.fail(ready.cause());
            }
        });
    }
}
