FROM openeuler/openeuler:23.03 as BUILDER
RUN dnf update -y && \
    dnf install -y golang && \
    go env -w GOPROXY=https://goproxy.cn,direct

MAINTAINER zengchen1024<chenzeng765@gmail.com>

# build binary
WORKDIR /go/src/github.com/opensourceways/robot-hook-dispatcher
COPY . .
RUN GO111MODULE=on CGO_ENABLED=0 go build -a -o robot-hook-dispatcher .

# copy binary config and utils
FROM openeuler/openeuler:22.03
RUN dnf -y update && \
    dnf in -y shadow && \
    groupadd -g 1000 hook-dispatcher && \
    useradd -u 1000 -g hook-dispatcher -s /bin/bash -m hook-dispatcher

USER hook-dispatcher

COPY  --from=BUILDER /go/src/github.com/opensourceways/robot-hook-dispatcher/robot-hook-dispatcher /opt/app/robot-hook-dispatcher

ENTRYPOINT ["/opt/app/robot-hook-dispatcher"]
