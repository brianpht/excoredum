package com.exadbe.gateway.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only index over the config-driven symbol registry. Preserves the
 * configured order for the {@code GET /symbols} response while giving O(1)
 * lookup by symbolId. Boundary-only (not the engine hot path).
 */
public final class SymbolRegistry {

    private final List<GatewayConfig.Symbol> ordered;
    private final Map<Integer, GatewayConfig.Symbol> byId;

    public SymbolRegistry(final List<GatewayConfig.Symbol> symbols) {
        final Map<Integer, GatewayConfig.Symbol> index = new LinkedHashMap<>();
        for (final GatewayConfig.Symbol s : symbols) {
            if (index.put(s.symbolId(), s) != null) {
                throw new IllegalArgumentException("duplicate symbolId in registry: " + s.symbolId());
            }
        }
        this.ordered = List.copyOf(symbols);
        this.byId = Map.copyOf(index);
    }

    /** Symbols in configured order. */
    public List<GatewayConfig.Symbol> ordered() {
        return ordered;
    }

    /** The symbol with the given id, or null when unknown. */
    public GatewayConfig.Symbol byId(final long symbolId) {
        return byId.get((int) symbolId);
    }
}
