FROM bitnami/spark:3.5

USER root

# JARs necessários para Delta Lake e acesso S3/MinIO (hadoop-aws + delta)
ENV DELTA_VERSION=3.2.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN install_packages curl python3-pip && \
    SPARK_JARS=/opt/bitnami/spark/jars && \
    curl -fsSL -o ${SPARK_JARS}/delta-spark_2.12-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -fsSL -o ${SPARK_JARS}/delta-storage-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -fsSL -o ${SPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL -o ${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

COPY ingestion/requirements.txt /tmp/ingestion-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/ingestion-requirements.txt

USER 1001
