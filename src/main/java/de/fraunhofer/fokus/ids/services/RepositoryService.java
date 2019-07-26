package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.persistence.entities.DataAsset;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class RepositoryService extends AbstractVerticle {

	private String repoPath;
	private EventBus eb;
	private String ROUTE_PREFIX = "de.fraunhofer.fokus.ids.";
	private Logger LOGGER = LoggerFactory.getLogger(RepositoryService.class.getName());

	@Override
	public void start(Future<Void> startFuture) {
		eb = vertx.eventBus();
		getConfig();

		eb.consumer(ROUTE_PREFIX+"ckan.repositoryService.getContent", receivedMessage -> {
			vertx.executeBlocking(future ->{
				future.complete(getFileContent(Json.decodeValue(receivedMessage.body().toString(), DataAsset.class)));
			}, res -> {
				receivedMessage.reply(res.result());
			});
		});
		eb.consumer(ROUTE_PREFIX+"ckan.repositoryService.deleteFile", receivedMessage -> {
			vertx.executeBlocking(future ->{
				deleteFile(Json.decodeValue(receivedMessage.body().toString(), DataAsset.class));
				future.complete();
			}, res -> {
				receivedMessage.reply(res.result());
			});
		});
		eb.consumer(ROUTE_PREFIX+"ckan.repositoryService.createFile", receivedMessage -> {
			vertx.executeBlocking(future ->{
				future.complete(createFile(Json.decodeValue(receivedMessage.body().toString(), URL.class)));
			}, res -> {
				receivedMessage.reply(Json.encode(res.result()));
			});
		});
	}

	public File createFile(URL url) {
		String ext = FilenameUtils.getExtension(url.getFile());
		String id = UUID.randomUUID().toString();
		int qIndex = ext.indexOf("?");
		if(qIndex != -1) {
			ext = ext.substring(0, ext.indexOf("?"));
		}
		String path = this.repoPath + "/" + id + "." + ext;
		File file = new File(path);
		return file;
	}

	public String getFileContent(DataAsset dataAsset) {
		String name = dataAsset.getAccessInformation();
		String content = null;
		try {
			content = new String(Files.readAllBytes(Paths.get(this.repoPath + "/" + name)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}

	public File getFile(String name) {
		File file = new File(this.repoPath + "/" + name);
		if(file.exists()) {
			return file;
		} else {
			return null;
		}
	}

	public void deleteFile(DataAsset dataAsset) {
		String name = dataAsset.getAccessInformation();
		File file = new File(this.repoPath + "/" + name);
		file.delete();
	}

	private void getConfig() {
		ConfigStoreOptions fileStore = new ConfigStoreOptions()
				.setType("file")
				.setConfig(new JsonObject().put("path", this.getClass().getClassLoader().getResource("conf/application.conf").getFile()));

		ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore);

		ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

		retriever.getConfig(ar -> {
			if (ar.succeeded()) {
				this.repoPath = ar.result().getJsonObject("repository").getString("path");
			} else {
				LOGGER.error("Config could not be retrieved.");
			}
		});
	}
}
