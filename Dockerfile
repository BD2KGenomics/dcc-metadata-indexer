FROM openjdk:8-jre-alpine
RUN apk add --update python py2-pip openssl ca-certificates py-openssl wget
RUN pip install --upgrade pip

RUN apk add --update bash
RUN apk add --update --no-cache gcc g++ py-lxml py-numpy
RUN apk add --update python2-dev libxml2-dev libxslt-dev curl-dev curl

RUN pip install numpy
WORKDIR /app

COPY . /app/dcc-metadata-indexer

ENV PYCURL_SSL_LIBRARY openssl

RUN cd /app/dcc-metadata-indexer\
  && mkdir /app/dcc-metadata-indexer/es-jsonls\
  && mkdir /app/dcc-metadata-indexer/redacted\
#  && pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil\
  && pip install -r requirements.txt\
  && cd /app


RUN mkdir /app/redwood-client\
  && cd redwood-client\
  && wget https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/20161216_ucsc-storage-client.tar.gz && tar zxf 20161216_ucsc-storage-client.tar.gz && rm -f 20161216_ucsc-storage-client.tar.gz\
  && cd /app


RUN cd /app/dcc-metadata-indexer
WORKDIR /app/dcc-metadata-indexer

RUN chmod a+x run.sh
ENTRYPOINT ["./run.sh"]

