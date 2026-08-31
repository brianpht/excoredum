package io.justrade.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit: the v5 index/count extension fields carry the full value past the
 * uint16 ceiling while the legacy field wraps, so a >65535-event command keeps
 * a correct intra-command index on the wire.
 */
class EventCodecExtRoundTripTest {

    private static final int ABOVE_UINT16 = 70_000;
    private static final int LEGACY_WRAPPED = ABOVE_UINT16 & 0xFFFF;

    @Test
    void commandResultEventCountExtRoundTrips() {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final CommandResultEncoder encoder = new CommandResultEncoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .commandIdHi(1L)
                .commandIdLo(2L)
                .resultCode(CommandResultCode.SUCCESS)
                .eventCount(ABOVE_UINT16)
                .eventCountExt(ABOVE_UINT16);

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final CommandResultDecoder decoder = new CommandResultDecoder();
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(LEGACY_WRAPPED, decoder.eventCount(), "legacy uint16 field wraps");
        assertEquals(ABOVE_UINT16, decoder.eventCountExt(), "extension field carries the full count");
    }

    @Test
    void journalEventIndexExtRoundTrips() {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final JournalEventEncoder encoder = new JournalEventEncoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .logPosition(9L)
                .eventIndex(ABOVE_UINT16)
                .eventIndexExt(ABOVE_UINT16)
                .timestamp(1L)
                .eventType(MatcherEventType.TRADE)
                .symbolId(1)
                .makerOrderId(1L)
                .makerUid(2L)
                .takerUid(3L)
                .price(4L)
                .size(5L)
                .makerCompleted((short) 0);

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final JournalEventDecoder decoder = new JournalEventDecoder();
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(LEGACY_WRAPPED, decoder.eventIndex(), "legacy uint16 field wraps");
        assertEquals(ABOVE_UINT16, decoder.eventIndexExt(), "extension field carries the full index");
    }

    @Test
    void tradeEventIndexExtRoundTrips() {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final TradeEventEncoder encoder = new TradeEventEncoder();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .commandIdHi(1L)
                .commandIdLo(2L)
                .eventIndex(ABOVE_UINT16)
                .eventIndexExt(ABOVE_UINT16)
                .timestamp(1L)
                .symbolId(1)
                .makerOrderId(1L)
                .makerUid(2L)
                .takerUid(3L)
                .price(4L)
                .size(5L)
                .makerOrderCompleted((short) 0);

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final TradeEventDecoder decoder = new TradeEventDecoder();
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        assertEquals(LEGACY_WRAPPED, decoder.eventIndex(), "legacy uint16 field wraps");
        assertEquals(ABOVE_UINT16, decoder.eventIndexExt(), "extension field carries the full index");
    }
}
