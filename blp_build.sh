#!/bin/bash

#build
#mvn clean package -pl seatunnel-dist -am -Dmaven.test.skip=true

current_dir=$(pwd)

zip_dir () {
  cd blp
  zip -r ../seatunnel.zip .
  cd ${current_dir}
}

set_lib_dir() {
 # we need the seatunnel-hadoop3-3.1.4-uber.jar and seatunnel-transforms-v2.jar in the lib folder
  cp seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar blp/lib
  cp seatunnel-transforms-v2/target/seatunnel-transforms-v2.jar blp/lib

  cd blp/lib

  # request by seatunnel s3 file connector
  wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar
  # this is required for hadoop
  wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar

  # iceberg 1.4.2
  wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.15/1.4.2/iceberg-flink-runtime-1.15-1.4.2.jar
  wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar
  # this is required for iceberg
  wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.14/bundle-2.20.14.jar

  cd ${current_dir}
}

clean_build () {
  rm -f seatunnel.zip
  rm -rf blp
  rm -rf connectors
}

build_plugins() {
  cd bin
  bash install-plugin.sh
  cd ${current_dir}
  cp -r connectors blp
  # this file is necessary, otherwise the plugins will not be found
  cp plugin-mapping.properties blp/connectors
}

clean_build

# set lib dir
mkdir -p blp/lib
set_lib_dir

# install plugins(s3, kafka, iceberg, fake, console), and copy
build_plugins

# copy the starter jar
mkdir -p blp/starter
cp seatunnel-core/seatunnel-flink-starter/seatunnel-flink-15-starter/target/seatunnel-flink-15-starter.jar blp/starter/

# zip the files
zip_dir

