# Multi-stage image for the justrade system test: builds every service
# distribution with the Gradle wrapper, then keeps a JRE runtime with the
# launcher, read replica, and bench (load + verify) distributions on board.
# The compose file picks the entrypoint per service.
#
# Base images are pinned by digest (not a floating tag) and the runtime runs as
# an unprivileged user, so the image is reproducible and not root-by-default.
FROM eclipse-temurin:21-jdk@sha256:85f00967bcc624fc19fa9c2cf124ea426a5363898e267141726f31f358c2e14b AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon \
    :launcher:installDist \
    :read:installDist \
    :bench:installDist \
    :examples:installDist

FROM eclipse-temurin:21-jre@sha256:7a65df4b22d2de92d4e04056e884f3b9122d70b21e2847fd66084278bd0ce037
RUN groupadd --gid 10001 justrade \
    && useradd --uid 10001 --gid justrade --no-create-home --home-dir /opt/justrade justrade \
    && mkdir -p /opt/justrade /data \
    && chown -R justrade:justrade /opt/justrade /data
COPY --from=build /src/launcher/build/install/launcher /opt/justrade/launcher
COPY --from=build /src/read/build/install/read /opt/justrade/read
COPY --from=build /src/bench/build/install/bench /opt/justrade/bench
COPY --from=build /src/examples/build/install/examples /opt/justrade/examples
COPY docker/entrypoint-node.sh \
    docker/entrypoint-read.sh \
    docker/entrypoint-load.sh \
    docker/entrypoint-verify.sh \
    /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint-node.sh \
    /usr/local/bin/entrypoint-read.sh \
    /usr/local/bin/entrypoint-load.sh \
    /usr/local/bin/entrypoint-verify.sh
# Aeron requires these module opens. JAVA_TOOL_OPTIONS rejects --add-opens, so
# the entrypoint scripts pass them on the java command line directly.
WORKDIR /opt/justrade
USER justrade
