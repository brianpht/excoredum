package com.exadbe.gateway.config;

import com.exadbe.protocol.QueryStreams;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Immutable configuration for the HTTP gateway: the HTTP bind address, the
 * read replica's query endpoint (and optional shared Aer on directory), the
 * cluster ingress for the write client, the admin uid allow-list, and the
 * config-driven symbol registry the UI uses to render prices.
 *
 * <p>Fully specified via the builder for tests, or via
 * {@link #fromProperties(Properties)} for the launcher.
 */
public final class GatewayConfig {

    /** One config-driven spot symbol. {@code name} is display-only (UI naming). */
    public record Symbol(
            int symbolId,
            String name,
            int baseCurrency,
            int quoteCurrency,
            long baseScaleK,
            long quoteScaleK,
            long makerFee,
            long takerFee) {}

    /**
     * One config-driven currency. {@code code} is display-only (UI naming) and
     * {@code scaleK} is the fixed-point divisor used to render amounts.
     */
    public record Currency(int id, String code, long scaleK) {}

    private final String httpHost;
    private final int httpPort;
    private final String readRequestChannel;
    private final int readRequestStreamId;
    private final int readResponseStreamId;
    private final String readAeronDir;
    private final long writeClientId;
    private final long writeInitialClientSeq;
    private final String writeIngressEndpoints;
    private final String writeEgressChannel;
    private final String writeAeronDir;
    private final long marketPumpIntervalMs;
    private final List<Long> adminUids;
    private final List<Symbol> symbols;
    private final List<Currency> currencies;

    private GatewayConfig(final Builder b) {
        this.httpHost = b.httpHost;
        this.httpPort = b.httpPort;
        this.readRequestChannel = b.readRequestChannel;
        this.readRequestStreamId = b.readRequestStreamId;
        this.readResponseStreamId = b.readResponseStreamId;
        this.readAeronDir = b.readAeronDir;
        this.writeClientId = b.writeClientId;
        this.writeInitialClientSeq = b.writeInitialClientSeq;
        this.writeIngressEndpoints = b.writeIngressEndpoints;
        this.writeEgressChannel = b.writeEgressChannel;
        this.writeAeronDir = b.writeAeronDir;
        this.marketPumpIntervalMs = b.marketPumpIntervalMs;
        this.adminUids = List.copyOf(b.adminUids);
        this.symbols = List.copyOf(b.symbols);
        this.currencies = List.copyOf(b.currencies);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Parses the gateway's operator-facing properties. */
    public static GatewayConfig fromProperties(final Properties p) {
        final Builder b = builder();
        b.httpHost(p.getProperty("gateway.http.host", "0.0.0.0"));
        b.httpPort(parseInt(p.getProperty("gateway.http.port", "8080"), "gateway.http.port"));
        b.readRequestChannel(p.getProperty("gateway.read.requestChannel", QueryStreams.QUERY_REQUEST_CHANNEL));
        b.readRequestStreamId(
                parseInt(p.getProperty("gateway.read.requestStreamId", "300"), "gateway.read.requestStreamId"));
        b.readResponseStreamId(
                parseInt(p.getProperty("gateway.read.responseStreamId", "301"), "gateway.read.responseStreamId"));
        b.readAeronDir(p.getProperty("gateway.read.aeronDir"));
        b.writeClientId(parseLong(p.getProperty("gateway.write.clientId", "1"), "gateway.write.clientId"));
        b.writeInitialClientSeq(
                parseLong(p.getProperty("gateway.write.initialClientSeq", "0"), "gateway.write.initialClientSeq"));
        b.writeIngressEndpoints(p.getProperty("gateway.write.ingressEndpoints", "localhost:20100"));
        b.writeEgressChannel(p.getProperty("gateway.write.egressChannel", "aeron:udp?endpoint=localhost:0"));
        b.writeAeronDir(p.getProperty("gateway.write.aeronDir"));
        b.marketPumpIntervalMs(
                parseLong(p.getProperty("gateway.marketPump.intervalMs", "1000"), "gateway.marketPump.intervalMs"));
        for (final String uid : p.getProperty("gateway.admin.uids", "").split(",")) {
            if (!uid.isBlank()) {
                b.adminUid(parseLong(uid.trim(), "gateway.admin.uids"));
            }
        }
        final String symbols = p.getProperty("gateway.symbols", "");
        if (!symbols.isBlank()) {
            for (final String token : symbols.split(",")) {
                b.symbol(parseSymbol(token));
            }
        }
        final String currencies = p.getProperty("gateway.currencies", "");
        if (!currencies.isBlank()) {
            for (final String token : currencies.split(",")) {
                b.currency(parseCurrency(token));
            }
        }
        return b.build();
    }

    /** Parses one {@code id,name,base,quote,baseScaleK,quoteScaleK[|makerFee|takerFee]} symbol token. */
    private static Symbol parseSymbol(final String token) {
        final String[] f = token.trim().split("\\|");
        if (f.length != 6 && f.length != 8) {
            throw new IllegalArgumentException(
                    "invalid gateway.symbols token (want id|name|base|quote|baseScaleK|quoteScaleK[|makerFee|takerFee]): "
                            + token);
        }
        return new Symbol(
                parseInt(f[0], "symbol.id"),
                f[1],
                parseInt(f[2], "symbol.base"),
                parseInt(f[3], "symbol.quote"),
                parseLong(f[4], "symbol.baseScaleK"),
                parseLong(f[5], "symbol.quoteScaleK"),
                f.length == 8 ? parseLong(f[6], "symbol.makerFee") : 0L,
                f.length == 8 ? parseLong(f[7], "symbol.takerFee") : 0L);
    }

    /** Parses one {@code id,code,scaleK} currency token. */
    private static Currency parseCurrency(final String token) {
        final String[] f = token.trim().split("\\|");
        if (f.length != 3) {
            throw new IllegalArgumentException("invalid gateway.currencies token (want id|code|scaleK): " + token);
        }
        final String code = f[1];
        final long scaleK = parseLong(f[2], "currency.scaleK");
        if (code.isBlank() || scaleK <= 0L) {
            throw new IllegalArgumentException("invalid gateway.currencies token: " + token);
        }
        return new Currency(parseInt(f[0], "currency.id"), code, scaleK);
    }

    private static int parseInt(final String s, final String key) {
        try {
            return Integer.parseInt(s);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("invalid integer for " + key + ": " + s, e);
        }
    }

    private static long parseLong(final String s, final String key) {
        try {
            return Long.parseLong(s);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("invalid long for " + key + ": " + s, e);
        }
    }

    public String httpHost() {
        return httpHost;
    }

    public int httpPort() {
        return httpPort;
    }

    public String readRequestChannel() {
        return readRequestChannel;
    }

    public int readRequestStreamId() {
        return readRequestStreamId;
    }

    public int readResponseStreamId() {
        return readResponseStreamId;
    }

    /** Shared Aer on directory for the read client, or null to launch an embedded media driver. */
    public String readAeronDir() {
        return readAeronDir;
    }

    public long writeClientId() {
        return writeClientId;
    }

    public long writeInitialClientSeq() {
        return writeInitialClientSeq;
    }

    public String writeIngressEndpoints() {
        return writeIngressEndpoints;
    }

    public String writeEgressChannel() {
        return writeEgressChannel;
    }

    /** Shared Aer on directory for the write client, or null to launch an embedded media driver. */
    public String writeAeronDir() {
        return writeAeronDir;
    }

    /** WebSocket market snapshot interval in ms; 0 disables the market pump. */
    public long marketPumpIntervalMs() {
        return marketPumpIntervalMs;
    }

    public List<Long> adminUids() {
        return adminUids;
    }

    public List<Symbol> symbols() {
        return symbols;
    }

    public List<Currency> currencies() {
        return currencies;
    }

    /** Fluent builder with conservative local defaults. */
    public static final class Builder {
        private String httpHost = "0.0.0.0";
        private int httpPort = 8080;
        private String readRequestChannel = QueryStreams.QUERY_REQUEST_CHANNEL;
        private int readRequestStreamId = QueryStreams.QUERY_REQUEST_STREAM_ID;
        private int readResponseStreamId = QueryStreams.QUERY_RESPONSE_STREAM_ID;
        private String readAeronDir;
        private long writeClientId = 1L;
        private long writeInitialClientSeq;
        private String writeIngressEndpoints = "localhost:20100";
        private String writeEgressChannel = "aeron:udp?endpoint=localhost:0";
        private String writeAeronDir;
        private long marketPumpIntervalMs = 1000L;
        private final List<Long> adminUids = new ArrayList<>();
        private final List<Symbol> symbols = new ArrayList<>();
        private final List<Currency> currencies = new ArrayList<>();

        public Builder httpHost(final String v) {
            this.httpHost = v;
            return this;
        }

        public Builder httpPort(final int v) {
            this.httpPort = v;
            return this;
        }

        public Builder readRequestChannel(final String v) {
            this.readRequestChannel = v;
            return this;
        }

        public Builder readRequestStreamId(final int v) {
            this.readRequestStreamId = v;
            return this;
        }

        public Builder readResponseStreamId(final int v) {
            this.readResponseStreamId = v;
            return this;
        }

        public Builder readAeronDir(final String v) {
            this.readAeronDir = v;
            return this;
        }

        public Builder writeClientId(final long v) {
            this.writeClientId = v;
            return this;
        }

        public Builder writeInitialClientSeq(final long v) {
            this.writeInitialClientSeq = v;
            return this;
        }

        public Builder writeIngressEndpoints(final String v) {
            this.writeIngressEndpoints = v;
            return this;
        }

        public Builder writeEgressChannel(final String v) {
            this.writeEgressChannel = v;
            return this;
        }

        public Builder writeAeronDir(final String v) {
            this.writeAeronDir = v;
            return this;
        }

        public Builder marketPumpIntervalMs(final long v) {
            this.marketPumpIntervalMs = v;
            return this;
        }

        public Builder adminUid(final long v) {
            this.adminUids.add(v);
            return this;
        }

        public Builder symbol(final Symbol v) {
            this.symbols.add(v);
            return this;
        }

        public Builder currency(final Currency v) {
            this.currencies.add(v);
            return this;
        }

        public GatewayConfig build() {
            if (httpPort < 0 || httpPort > 65535) {
                throw new IllegalArgumentException("httpPort out of range: " + httpPort);
            }
            return new GatewayConfig(this);
        }
    }
}
