FROM alpine:3.5
RUN apk add --update python py2-pip openssl ca-certificates py-openssl wget
RUN pip install --upgrade pip

RUN apk add --update bash
RUN apk add --update --no-cache gcc g++ py-lxml py-numpy
RUN apk add --update python2-dev libxml2-dev libxslt-dev

RUN pip install numpy
#RUN apk --update add --virtual build-dependencies libffi-dev openssl-dev python-dev  build-base\
#  && apk --update add libxml2-dev libxslt-dev\
#  && pip install virtualenv\
#  && pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil
#  && apk del build-dependencies

WORKDIR /app

COPY . /app/dcc-metadata-indexer

RUN cd /app/dcc-metadata-indexer\
  && mkdir /app/dcc-metadata-indexer/es-jsonls\
  && mkdir /app/dcc-metadata-indexer/redacted\
#  && . env/bin/activate\
  && pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil\
#  && deactivate\
  && cd /app

#VOLUME /app/dcc-metadata-indexer/es-jsonls

RUN mkdir /app/redwood-client\
  && cd redwood-client\
  && wget https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/20161216_ucsc-storage-client.tar.gz && tar zxf 20161216_ucsc-storage-client.tar.gz && rm -f 20161216_ucsc-storage-client.tar.gz\
  && cd /app


ENTRYPOINT ["python", "/app/dcc-metadata-indexer/metadata_indexer.py", "--client-path", "/app/redwood-client/ucsc-storage-client/", "--metadata-schema", "/app/dcc-metadata-indexer/metadata_schema.json"]

#CMD ["--skip-program", "TEST", "--skip-project", "TEST", "--storage-access-token", ""]

RUN echo "Copy JSON files to a different folder so they are available for mounting and exposed to the world."

#Copy all the jsonl files to the folder "es-jsonls" so they are available upon mounting.
RUN find . -name "*.jsonl" -exec cp {} /app/dcc-metadata-indexer/es-jsonls \;
