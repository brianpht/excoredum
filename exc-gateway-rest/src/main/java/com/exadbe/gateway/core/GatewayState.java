package com.exadbe.gateway.core;

import java.util.Collection;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * The gateway's in-memory registry: asset and symbol specs (code to core id
 * mapping plus scales) and per-user order profiles. All access happens on the
 * gateway agent thread, so no synchronization is needed. The registry is not
 * persisted; a gateway restart requires re-registering assets and symbols,
 * mirroring the reference implementation.
 */
public final class GatewayState {

    private static final float LOAD_FACTOR = 0.65f;

    private final Object2ObjectHashMap<String, GatewayAssetSpec> assetsByCode;
    private final Int2ObjectHashMap<GatewayAssetSpec> assetsById;
    private final Object2ObjectHashMap<String, GatewaySymbolSpec> symbolsByCode;
    private final Int2ObjectHashMap<GatewaySymbolSpec> symbolsById;
    private final Long2ObjectHashMap<GatewayUserProfile> profiles;

    public GatewayState(final int registryCapacity) {
        this.assetsByCode = new Object2ObjectHashMap<>(registryCapacity, LOAD_FACTOR);
        this.assetsById = new Int2ObjectHashMap<>(registryCapacity, LOAD_FACTOR);
        this.symbolsByCode = new Object2ObjectHashMap<>(registryCapacity, LOAD_FACTOR);
        this.symbolsById = new Int2ObjectHashMap<>(registryCapacity, LOAD_FACTOR);
        this.profiles = new Long2ObjectHashMap<>(registryCapacity, LOAD_FACTOR);
    }

    public GatewayAssetSpec getAssetSpec(final String assetCode) {
        return assetCode == null ? null : assetsByCode.get(assetCode);
    }

    public GatewayAssetSpec getAssetSpec(final int assetId) {
        return assetsById.get(assetId);
    }

    /** Registers a new asset; returns false when the code or id already exists. */
    public boolean registerNewAsset(final GatewayAssetSpec spec) {
        if (assetsByCode.containsKey(spec.assetCode()) || assetsById.containsKey(spec.assetId())) {
            return false;
        }
        assetsByCode.put(spec.assetCode(), spec);
        assetsById.put(spec.assetId(), spec);
        return true;
    }

    public Collection<GatewayAssetSpec> assets() {
        return assetsById.values();
    }

    public GatewaySymbolSpec getSymbolSpec(final String symbolCode) {
        return symbolCode == null ? null : symbolsByCode.get(symbolCode);
    }

    public GatewaySymbolSpec getSymbolSpec(final int symbolId) {
        return symbolsById.get(symbolId);
    }

    /** Registers a new symbol in NEW state; returns false when code or id already exists. */
    public boolean registerNewSymbol(final GatewaySymbolSpec spec) {
        if (symbolsByCode.containsKey(spec.symbolCode()) || symbolsById.containsKey(spec.symbolId())) {
            return false;
        }
        symbolsByCode.put(spec.symbolCode(), spec);
        symbolsById.put(spec.symbolId(), spec);
        return true;
    }

    /** Promotes a NEW symbol to ACTIVE once the core acknowledges ADD_SYMBOL. */
    public GatewaySymbolSpec activateSymbol(final int symbolId) {
        final GatewaySymbolSpec spec = symbolsById.get(symbolId);
        if (spec != null && spec.status() == GatewaySymbolSpec.STATUS_NEW) {
            spec.status(GatewaySymbolSpec.STATUS_ACTIVE);
        }
        return spec;
    }

    /** Drops a symbol registration after the core rejects ADD_SYMBOL. */
    public void removeSymbol(final int symbolId) {
        final GatewaySymbolSpec removed = symbolsById.remove(symbolId);
        if (removed != null) {
            symbolsByCode.remove(removed.symbolCode());
        }
    }

    public Collection<GatewaySymbolSpec> symbols() {
        return symbolsById.values();
    }

    public GatewayUserProfile getUserProfile(final long uid) {
        return profiles.get(uid);
    }

    public GatewayUserProfile getOrCreateUserProfile(final long uid) {
        GatewayUserProfile profile = profiles.get(uid);
        if (profile == null) {
            profile = new GatewayUserProfile(uid);
            profiles.put(uid, profile);
        }
        return profile;
    }
}
