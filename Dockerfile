FROM adoptopenjdk/openjdk15:alpine
RUN apk --no-cache add curl
COPY target/rawdata-converter-app-migration-*.jar rawdata-converter-app-migration.jar
COPY target/classes/logback*.xml /conf/
EXPOSE 8080
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005", "-Dcom.sun.management.jmxremote", "-Xmx1g", "-jar", "rawdata-converter-app-migration.jar"]
