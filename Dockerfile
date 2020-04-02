FROM golang:1-alpine

ENV BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo."

COPY . /$GOPATH/src/github.com/Financial-Times/elasticsearch-reindexer/

RUN apk --update add git libc-dev ca-certificates \
  && cd /$GOPATH/src/github.com/Financial-Times/elasticsearch-reindexer \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && echo $LDFLAGS \
  && go build -mod=readonly -ldflags="${LDFLAGS}" \
  && mv elasticsearch-reindexer /elasticsearch-reindexer \
  && mv startup.sh /startup.sh \
  && cd / \
  && chmod +x startup.sh \
  && apk del libc-dev \
  && rm -rf $GOPATH /var/cache/apk/*

ONBUILD COPY . /index-mapping/

ONBUILD RUN cd /index-mapping \
  && echo "$(git describe --tag --always 2> /dev/null)" > /mapping.version \
  && cp /index-mapping/mapping.json / \
  && if [ -f /index-mapping/alias-filter.json ]; then cp /index-mapping/alias-filter.json / ; fi \
  && apk del git \
  && rm -rf /index-mapping

WORKDIR /

CMD [ "/startup.sh" ]
