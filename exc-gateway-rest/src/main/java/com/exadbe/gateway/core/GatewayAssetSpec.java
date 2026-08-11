package com.exadbe.gateway.core;

/**
 * Gateway-local asset definition mapping a currency code to the integer id the
 * core knows, plus the number of decimal places its REST amounts carry.
 * Registered via the admin API; never sent to the cluster.
 */
public final class GatewayAssetSpec {

    private final String assetCode;
    private final int assetId;
    private final int scale;
    private boolean active;

    public GatewayAssetSpec(final String assetCode, final int assetId, final int scale, final boolean active) {
        this.assetCode = assetCode;
        this.assetId = assetId;
        this.scale = scale;
        this.active = active;
    }

    public String assetCode() {
        return assetCode;
    }

    public int assetId() {
        return assetId;
    }

    /** Number of decimal places amounts in this asset carry at the REST boundary. */
    public int scale() {
        return scale;
    }

    public boolean active() {
        return active;
    }

    public void active(final boolean value) {
        this.active = value;
    }
}
