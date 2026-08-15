# Multi-stage image for the excoredum system test: builds every service
# distribution with the Gradle wrapper, then keeps a JRE runtime with the
# launcher, read replica, and bench (load + verify) distributions on board.
# The compose file picks the entrypoint per service.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon \
    :exc-launcher:installDist \
    :exc-read:installDist \
    :exc-bench:installDist \
    :exc-examples:installDist

FROM eclipse-temurin:21-jre
RUN mkdir -p /opt/excoredum /data
COPY --from=build /src/exc-launcher/build/install/exc-launcher /opt/excoredum/launcher
COPY --from=build /src/exc-read/build/install/exc-read /opt/excoredum/read
COPY --from=build /src/exc-bench/build/install/exc-bench /opt/excoredum/bench
COPY --from=build /src/exc-examples/build/install/exc-examples /opt/excoredum/examples
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
WORKDIR /opt/excoredum
