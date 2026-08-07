package com.exadbe.engine.risk;

import org.agrona.collections.Int2ObjectHashMap;

/** Single-writer store of symbol specifications, keyed by symbolId. */
public final class SymbolSpecStore {

    private final Int2ObjectHashMap<SymbolSpec> byId = new Int2ObjectHashMap<>();

    /** Adds a spec; returns {@code false} if the symbol already exists. */
    public boolean add(final SymbolSpec spec) {
        if (byId.containsKey(spec.symbolId())) {
            return false;
        }
        byId.put(spec.symbolId(), spec);
        return true;
    }

    public SymbolSpec get(final int symbolId) {
        return byId.get(symbolId);
    }

    public boolean contains(final int symbolId) {
        return byId.containsKey(symbolId);
    }

    public int size() {
        return byId.size();
    }
}
