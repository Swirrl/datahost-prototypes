FROM cimg/clojure:1.11.1

ENV HURL_VERSION="4.1.0"

RUN curl --silent --location https://github.com/Orange-OpenSource/hurl/releases/download/$HURL_VERSION/hurl-$HURL_VERSION-x86_64-unknown-linux-gnu.tar.gz | sudo tar xvz -C /opt && sudo mv /opt/hurl-$HURL_VERSION-x86_64-unknown-linux-gnu /opt/hurl

ENV PATH=$PATH:/opt/hurl
