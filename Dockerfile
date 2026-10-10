FROM python:3.12-slim-trixie AS build
WORKDIR /build
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
RUN python -m pip wheel --no-cache-dir --no-deps --wheel-dir /wheels .

FROM python:3.12-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=build /wheels /wheels
RUN python -m pip install --no-cache-dir --no-deps --no-index /wheels/*.whl \
    && rm -rf /wheels \
    && mkdir /state \
    && chown 10001:10001 /state
COPY LICENSE NOTICE /usr/share/doc/xordination/
USER 10001:10001
WORKDIR /state
VOLUME ["/state"]
EXPOSE 8765
ENTRYPOINT ["xordination"]
CMD ["serve", "--state", "/state/broker.sqlite3", "--host", "0.0.0.0", "--port", "8765"]

