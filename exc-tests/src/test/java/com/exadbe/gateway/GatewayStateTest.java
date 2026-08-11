package com.exadbe.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.exadbe.gateway.core.GatewayAssetSpec;
import com.exadbe.gateway.core.GatewayOrder;
import com.exadbe.gateway.core.GatewayState;
import com.exadbe.gateway.core.GatewaySymbolSpec;
import com.exadbe.gateway.core.GatewayUserProfile;
import org.junit.jupiter.api.Test;

/** Gateway registry semantics: asset/symbol uniqueness, lifecycle, and profiles. */
class GatewayStateTest {

    private static final int CAPACITY = 16;

    private static GatewayAssetSpec asset(final String code, final int id, final int scale) {
        return new GatewayAssetSpec(code, id, scale, true);
    }

    private static GatewaySymbolSpec symbol(
            final int id, final String code, final GatewayAssetSpec base, final GatewayAssetSpec quote) {
        return new GatewaySymbolSpec(
                id, code, "CURRENCY_EXCHANGE_PAIR", base, quote, 1L, 1L, 0L, 0L, GatewaySymbolSpec.STATUS_NEW);
    }

    @Test
    void registersAssetsByCodeAndId() {
        final GatewayState state = new GatewayState(CAPACITY);
        final GatewayAssetSpec btc = asset("BTC", 10, 8);

        assertTrue(state.registerNewAsset(btc));
        assertSame(btc, state.getAssetSpec("BTC"));
        assertSame(btc, state.getAssetSpec(10));

        assertFalse(state.registerNewAsset(asset("BTC", 11, 2)), "duplicate code rejected");
        assertFalse(state.registerNewAsset(asset("ETH", 10, 2)), "duplicate id rejected");
    }

    @Test
    void registersSymbolsAndActivatesThem() {
        final GatewayState state = new GatewayState(CAPACITY);
        final GatewayAssetSpec base = asset("BTC", 10, 8);
        final GatewayAssetSpec quote = asset("USD", 20, 2);
        state.registerNewAsset(base);
        state.registerNewAsset(quote);

        final GatewaySymbolSpec spec = symbol(1, "BTCUSD", base, quote);
        assertTrue(state.registerNewSymbol(spec));
        assertEquals(GatewaySymbolSpec.STATUS_NEW, state.getSymbolSpec("BTCUSD").status());

        assertFalse(state.registerNewSymbol(symbol(1, "ETHUSD", base, quote)), "duplicate id rejected");
        assertFalse(state.registerNewSymbol(symbol(2, "BTCUSD", base, quote)), "duplicate code rejected");

        assertSame(spec, state.activateSymbol(1));
        assertEquals(GatewaySymbolSpec.STATUS_ACTIVE, state.getSymbolSpec(1).status());

        state.removeSymbol(1);
        assertNull(state.getSymbolSpec(1));
        assertNull(state.getSymbolSpec("BTCUSD"));
    }

    @Test
    void managesUserProfilesAndOrders() {
        final GatewayState state = new GatewayState(CAPACITY);
        assertNull(state.getUserProfile(1L));

        final GatewayUserProfile profile = state.getOrCreateUserProfile(1L);
        assertSame(profile, state.getOrCreateUserProfile(1L));
        assertSame(profile, state.getUserProfile(1L));

        final GatewayOrder order =
                new GatewayOrder(42L, "BTCUSD", 1, true, "GTC", 0L, 100L, 10L, GatewayOrder.STATE_NEW, 7L);
        profile.addOrder(order);
        assertSame(order, profile.order(42L));
        assertEquals("NEW", order.stateName());

        order.state(GatewayOrder.STATE_ACTIVE);
        order.addDeal(true, 100L, 4L);
        assertEquals(4L, order.filled());
        assertEquals(1, order.deals().size());
        assertEquals("ACTIVE", GatewayOrder.stateName(order.state()));

        assertSame(order, profile.removeOrder(42L));
        assertNull(profile.order(42L));
    }

    @Test
    void exposesRegistryViewsForInfo() {
        final GatewayState state = new GatewayState(CAPACITY);
        final GatewayAssetSpec base = asset("BTC", 10, 8);
        state.registerNewAsset(base);
        state.registerNewSymbol(symbol(1, "BTCUSD", base, asset("USD", 20, 2)));

        assertEquals(1, state.assets().size());
        assertEquals(1, state.symbols().size());
        assertNotNull(state.assets().iterator().next());
    }
}
