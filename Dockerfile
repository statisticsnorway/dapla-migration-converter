#FROM eu.gcr.io/prod-bip/alpine-jdk15-buildtools:master-7744b1c6a23129ceaace641d6d76d0a742440b58 as build

#
# Build stripped JVM
#
#RUN ["jlink", "--strip-java-debug-attributes", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
# "--add-modules", "java.base,java.management,jdk.management.agent,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki,jdk.jcmd,jdk.jartool,jdk.jdi,jdk.jfr"]

#
# Build Application image
#
FROM adoptopenjdk/openjdk15

#RUN apk --no-cache add curl tar gzip nano jq libc6-compat gcompat
RUN apt-get update && \
    apt-get -y --no-install-recommends install curl tar gzip nano jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists

#
# Resources from build image
#
#COPY --from=build /opt/jdk /jdk/
#COPY --from=build /opt/jdk/bin/jar /opt/jdk/bin/jcmd /opt/jdk/bin/jdb /opt/jdk/bin/jfr /opt/jdk/bin/jinfo /opt/jdk/bin/jmap /opt/jdk/bin/jps /opt/jdk/bin/jstack /opt/jdk/bin/jstat /jdk/bin/
COPY target/libs /app/lib/
COPY target/dapla-migration-converter-*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-prod-bip.xml /app/conf/
COPY conf/application-dc.yml /app/conf/application-dc.yml
COPY conf/bootstrap-dc.yml /app/conf/bootstrap-dc.yml
COPY conf/dummy.json /app/conf/private/dummy.json

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10320

CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005", "-Dcom.sun.management.jmxremote", "-cp", "/app/lib/*", "no.ssb.rawdata.converter.app.migration.Application"]
