FROM maven:3.6.0-jdk-11-slim
WORKDIR /home/app/
COPY src/ ./src
COPY pom.xml ./
RUN mvn -f ./pom.xml clean package
EXPOSE 8080
ENTRYPOINT ["java","-jar","./target/odc-adapter-ckan-1.0-SNAPSHOT-fat.jar"]
