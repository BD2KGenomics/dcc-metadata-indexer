FROM quay.io/ucsc_cgl/redwood-client:1.1.0

RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y python-dev libxml2-dev libxslt-dev lib32z1-dev libpq-dev
RUN apt-get install -y python-pip wget
RUN pip install -U pip
RUN pip install --upgrade cffi

WORKDIR /app
COPY . /app/dcc-metadata-indexer

RUN cd /app/dcc-metadata-indexer\
  && mkdir /app/dcc-metadata-indexer/es-jsonls\
  && mkdir /app/dcc-metadata-indexer/redacted\
  && pip install -r requirements.txt\
  && cd /app

RUN cd /app/dcc-metadata-indexer
WORKDIR /app/dcc-metadata-indexer

RUN chmod a+x run.sh
EXPOSE 9200

#Cron tab preparation
# Add crontab file in the cron directory
ADD crontab /etc/cron.d/indexer-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/indexer-cron

ENTRYPOINT ["./run2.sh"] 


