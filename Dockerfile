FROM ubuntu:24.04 AS base
RUN apt-get update && apt-get install -y wget apt-transport-https gpg \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/java \
    && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
    | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" \
    | tee /etc/apt/sources.list.d/adoptium.list \
    && apt update && apt install -y temurin-21-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --display java

ENV JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-arm64
ENV PATH="$JAVA_HOME/bin:${PATH}"

FROM base AS builder
ENV MAVEN_HOME=/opt/maven
RUN mkdir -p $MAVEN_HOME \
    && wget -q https://dlcdn.apache.org/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz \
    && tar -zxf apache-maven-3.9.11-bin.tar.gz -C $MAVEN_HOME --strip-components=1 \
    && rm apache-maven-3.9.11-bin.tar.gz
ENV PATH="$MAVEN_HOME/bin:${PATH}"

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests

FROM base AS runtime
WORKDIR /app

COPY --from=builder /app/target/lark-saga-bus-consumer-1.0-SNAPSHOT.jar app.jar
EXPOSE 8001
ENTRYPOINT ["java", "-jar", "app.jar"]
