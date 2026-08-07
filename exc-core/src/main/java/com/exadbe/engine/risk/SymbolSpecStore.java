package com.exadbe.engine.risk;

import java.util.Arrays;
import org.agrona.collections.Int2ObjectHashMap;

/** Single-writer store of symbol specifications, keyed by symbolId. */
public final class SymbolSpecStore {

    private final Int2ObjectHashMap<SymbolSpec> byId = new Int2ObjectHashMap<>();
    private int[] idScratch = new int[0];

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

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        byId.clear();
    }

    /**
     * Emits every spec in ascending {@code symbolId} order.
     *
     * <p>Cold snapshot path: key extraction and sorting are acceptable here.
     */
    public void forEachSorted(final SpecConsumer consumer) {
        final int count = byId.size();
        if (idScratch.length < count) {
            idScratch = new int[count];
        }
        final int[] ids = idScratch;
        final int[] cursor = {0};
        byId.forEachInt((id, spec) -> ids[cursor[0]++] = id);
        Arrays.sort(ids, 0, count);
        for (int i = 0; i < count; i++) {
            consumer.accept(byId.get(ids[i]));
        }
    }

    /** Callback for deterministic symbol-spec iteration. */
    @FunctionalInterface
    public interface SpecConsumer {
        void accept(SymbolSpec spec);
    }
}
