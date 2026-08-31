package com.exadbe.config;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.exadbe.collections.DedupRing;
import org.junit.jupiter.api.Test;

/** CoreConfig validates capacities at construction so a bad size fails fast, not mid-run. */
class CoreConfigTest {

    @Test
    void defaultsAreValid() {
        CoreConfig.defaults();
    }

    @Test
    void dedupWindowMustBeAPowerOfTwo() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().dedupWindow(1000).build());
    }

    @Test
    void journalSlotCountMustBeAPowerOfTwo() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().journalSlotCount(1000).build());
    }

    @Test
    void capacitiesMustBePositive() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().accountCapacity(-1).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().orderPoolCapacity(0).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().eventBufferCapacity(0).build());
    }

    @Test
    void journalSlotSizeMustExceedTheLengthHeader() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CoreConfig.builder().journalSlotSize(4).build());
    }

    @Test
    void dedupRingRequiresAPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new DedupRing(3));
        assertThrows(IllegalArgumentException.class, () -> new DedupRing(0));
    }
}
