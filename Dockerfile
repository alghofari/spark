FROM apache/spark:v3.3.2

# change into user root
USER root

# install sbt
RUN apt-get update && \
    apt-get install -yqq apt-transport-https curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# copy project
WORKDIR /opt/app/spark
COPY project project
COPY src src
COPY build.sbt build.sbt
COPY version.sbt version.sbt

# build the application using sbt
RUN sbt assembly