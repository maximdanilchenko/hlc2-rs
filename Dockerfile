FROM scratch

ADD target/x86_64-unknown-linux-musl/debug/high-load-2-rst /
EXPOSE 80

ENV PROD=true

CMD ["/high-load-2-rst"]
