FROM alpine:latest
MAINTAINER Tony tony.g.dbm@gmail.com

WORKDIR /data
ADD requirements.txt .
ADD config .
ADD python_client.py .
ADD ceph /etc/ceph
VOLUME ["/data"]
USER root

RUN echo "https://mirrors.aliyun.com/alpine/v3.10/main" > /etc/apk/repositories \
    && echo "https://mirrors.aliyun.com/alpine/v3.10/community"  >> /etc/apk/repositories \
    && apk update \
    && apk add --no-cache python \
    && apk add --no-cache python-dev \
    && python -m ensurepip \
    && if [ ! -e /usr/bin/pip ]; then ln -s pip /usr/bin/pip ; fi \
    && if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python /usr/bin/python; fi \
    && pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ \
    && pip install --no-cache-dir kubernetes -i https://mirrors.aliyun.com/pypi/simple/ \
    && apk add --no-cache ceph-common \
    && rm -rf /var/cache/apk/* \
    && rm -rf ~/.cache/pip

ENTRYPOINT [ "/bin/sh" ]

