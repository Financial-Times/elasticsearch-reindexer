FROM golang:1 as builder

ENV PROJECT=elasticsearch-reindexer
ENV BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo."

COPY . /${PROJECT}/
WORKDIR /${PROJECT}

RUN VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && echo $LDFLAGS \
  && CGO_ENABLED=0 go build -mod=readonly -o /artifacts/${PROJECT} -ldflags="${LDFLAGS}"



FROM alpine:3

COPY --from=builder /artifacts/* /
COPY ./startup.sh /

ONBUILD COPY . /index-mapping/

ONBUILD RUN apk --update add git ca-certificates \
  && cd /index-mapping \
  && echo "$(git describe --tag --always 2> /dev/null)" > /mapping.version \
  && cp /index-mapping/mapping.json / \
  && if [ -f /index-mapping/alias-filter.json ]; then cp /index-mapping/alias-filter.json / ; fi \
  && apk del git \
  && rm -rf /index-mapping

WORKDIR /
RUN chmod +x startup.sh

CMD [ "/startup.sh" ]
