FROM maven:3.6.0-jdk-11-slim
RUN groupadd -r ids && useradd -r -g ids ids
WORKDIR /home/app/
RUN chown ids: ./ && chmod u+w ./
RUN mkdir -p /ids/repo/ && chown ids: /ids/repo/ && chmod u+w /ids/repo/
COPY src/ ./src
COPY pom.xml ./
RUN mvn -f ./pom.xml clean package
EXPOSE 8080
USER ids
ENTRYPOINT ["java","-jar","./target/odc-adapter-ckan-1.0-SNAPSHOT-fat.jar"]
