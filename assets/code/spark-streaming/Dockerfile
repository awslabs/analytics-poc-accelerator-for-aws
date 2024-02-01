FROM public.ecr.aws/amazoncorretto/amazoncorretto:8-al2023

RUN yum install -y maven unzip
ENV JAVA_HOME="/usr/lib/jvm/java"

WORKDIR /build

RUN curl -LO https://github.com/awslabs/spark-sql-kinesis-connector/archive/refs/tags/v1.0.0.zip && \
    unzip v1.0.0.zip && \
    cd spark-sql-kinesis-connector-1.0.0 && \
    mvn clean package -DskipTests && \
    rm ../v1.0.0.zip