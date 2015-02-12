#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create snapshot builds with:
# docker build -t spark:git-`git rev-parse --short HEAD` .

# Basing from Mesos image so the Mesos native library is present.
FROM mesosphere/mesos:0.21.1-1.1.ubuntu1404
MAINTAINER Timothy Chen <tnachen@apache.org>

# Set environment variables.
ENV DEBIAN_FRONTEND "noninteractive"
ENV DEBCONF_NONINTERACTIVE_SEEN "true"

# Upgrade package index and install basic commands.
RUN apt-get update && \
    apt-get install -y openjdk-6-jdk maven

ENV JAVA_HOME /usr/lib/jvm/java-6-openjdk-amd64

ENV MESOS_NATIVE_LIBRARY /usr/local/lib/libmesos.so

ADD . /opt/spark/src

WORKDIR /opt/spark/src

RUN ./make-distribution.sh

RUN mv dist /opt/spark && rm -rf /opt/spark/src

WORKDIR /opt/spark/dist