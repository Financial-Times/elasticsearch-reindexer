FROM alpine:3.5

COPY . /gopath/src/github.com/Financial-Times/elasticsearch-reindexer/

ONBUILD COPY . /index-mapping/

WORKDIR /gopath/src/github.com/Financial-Times/elasticsearch-reindexer

ONBUILD RUN  apk --update add git go libc-dev ca-certificates \
  && export GOPATH=$(pwd | sed "s/\/src\/github\.com\/.*$//") \
  && BUILDINFO_PACKAGE="github.com/Financial-Times/elasticsearch-reindexer/vendor/github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && cd /index-mapping \
  && INDEX_VERSION="github.com/Financial-Times/elasticsearch-reindexer/service.indexVersion=$(git describe --tag --always 2> /dev/null)" \
  && cd - \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"' -X '"${INDEX_VERSION}"'" \
  && echo $LDFLAGS \
  && go get -u github.com/kardianos/govendor \
  && $GOPATH/bin/govendor sync \
  && go build -ldflags="${LDFLAGS}" \
  && mv elasticsearch-reindexer /elasticsearch-reindexer \
  && cd / \
  && cp /index-mapping/mapping.json / \
  && apk del go git libc-dev \
  && rm -rf $GOPATH /var/cache/apk/* /index-mapping

CMD [ "/elasticsearch-reindexer" ]
