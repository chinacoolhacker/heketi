# make image for building go app

# need to set 26 insead of latest 
FROM fedora:26 as builder 

ENV BUILD_HOME=/build
ENV GOPATH=$BUILD_HOME/golang
ENV PATH=$GOPATH/bin:$PATH
ENV HEKETI_BRANCH="devel"

# install dependencies, build and cleanup
RUN mkdir $BUILD_HOME $GOPATH && \
    dnf -y install glide golang git make && \
    dnf -y clean all 

RUN mkdir -p $GOPATH/src/github.com/chinacoolhacker && \
    cd $GOPATH/src/github.com/chinacoolhacker && \
    git clone -b $HEKETI_BRANCH https://github.com/chinacoolhacker/heketi.git && \
    cd $GOPATH/src/github.com/chinacoolhacker/heketi && \
    glide install -v 

RUN cd $GOPATH/src/github.com/chinacoolhacker/heketi && \
    git pull && \
    make && \
    cp heketi /usr/bin/heketi && \
    cp client/cli/go/heketi-cli /usr/bin/heketi-cli 



# post install config and volume setup
FROM alpine:latest

LABEL version="0.0.9"
LABEL description="Development build"

ADD ./heketi.json /etc/heketi/heketi.json
ADD ./heketi-start.sh /usr/bin/heketi-start.sh

RUN mkdir /var/lib/heketi && \
    apk add --no-cache --update ca-certificates 

VOLUME /etc/heketi
VOLUME /var/lib/heketi

# copy bin from builder 
COPY --from=builder /usr/bin/heketi /usr/bin/heketi 
COPY --from=builder /usr/bin/heketi-cli /usr/bin/heketi-cli

# expose port, set user and set entrypoint with config option
ENTRYPOINT ["/usr/bin/heketi-start.sh"]
EXPOSE 8080

