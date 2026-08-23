package com.exadbe.read;

import com.exadbe.config.CoreConfig;
import com.exadbe.engine.orderbook.L2View;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderType;
import com.exadbe.protocol.QueryRequestDecoder;
import com.exadbe.protocol.QueryResponseEncoder;
import com.exadbe.protocol.QueryStatusCode;
import com.exadbe.protocol.QueryType;
import com.exadbe.read.config.ReadReplicaConfig;
import com.exadbe.read.order.Fill;
import com.exadbe.read.order.MarketTrade;
import com.exadbe.read.order.OrderRecord;
import com.exadbe.read.report.SingleUserReport;
import com.exadbe.read.report.TotalCurrencyBalance;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Serves the read-side query protocol on a replica's polling thread: subscribes
 * to {@link QueryRequest} frames, answers each from the replica's replicated
 * state on the same single thread that advances replication, and publishes a
 * {@link QueryResponse} to the client's ephemeral response subscription.
 *
 * <p>Single-writer: {@link #poll()} must be called from the replica's polling
 * thread (the same thread that calls {@code ExcReadReplica#poll()}), so the
 * engine and ledger are only ever touched by one thread. Responses are encoded
 * into a preallocated buffer with an exact per-element budget check; a response
 * that would overflow is truncated with status {@link QueryStatusCode#TRUNCATED}
 * rather than corrupting the buffer.
 */
public final class QueryResponder implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int RESPONSE_BUFFER_CAPACITY = 256 * 1024;
    private static final int MAX_RESPONSE_PUBLICATIONS = 64;
    private static final int RESPONSE_PUBLICATIONS_MASK = MAX_RESPONSE_PUBLICATIONS - 1;
    private static final int MAX_CHANNEL_LENGTH = 64;
    private static final int GROUP_SIZE_ENCODING_LENGTH = 4;

    private static final byte[] UDP_CHANNEL_PREFIX = {'a', 'e', 'r', 'o', 'n', ':', 'u', 'd', 'p'};
    private static final byte[] IPC_CHANNEL_PREFIX = {'a', 'e', 'r', 'o', 'n', ':', 'i', 'p', 'c'};

    private final ExcReadReplica replica;
    private final Aeron aeron;
    private final Subscription requests;
    private final FragmentAssembler requestAssembler;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final QueryRequestDecoder requestDecoder = new QueryRequestDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryResponseEncoder responseEncoder = new QueryResponseEncoder();
    private final UnsafeBuffer responseBuffer = new UnsafeBuffer(new byte[RESPONSE_BUFFER_CAPACITY]);
    private final L2View l2View = new L2View(CoreConfig.DEFAULT_L2_MAX_LEVELS);

    // Bounded LRU of response publications, keyed by a composite of the channel
    // bytes and stream id, so answering a query allocates nothing (the old
    // String decode + concat key churned per request). The channel bytes are
    // retained for collision-safe verification; the String form is decoded only
    // when a new publication must be created.
    private final long[] publicationKeys = new long[MAX_RESPONSE_PUBLICATIONS];
    private final byte[][] publicationChannels = new byte[MAX_RESPONSE_PUBLICATIONS][];
    private final Publication[] publicationSlots = new Publication[MAX_RESPONSE_PUBLICATIONS];
    private final byte[] channelScratch = new byte[MAX_CHANNEL_LENGTH];
    private int publicationHead;
    private int publicationCount;

    private long replies;
    private long dropped;
    private long received;

    /**
     * @param replica the replica serving queries; must already be constructed
     * @param config replica configuration, whose media driver directory this
     *     responder shares so its own Aeron client talks to the same driver
     */
    public QueryResponder(final ExcReadReplica replica, final ReadReplicaConfig config) {
        this.replica = replica;
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectoryName()));
        this.requestAssembler = new FragmentAssembler(this::onRequest);
        try {
            this.requests = aeron.addSubscription(config.queryRequestChannel(), config.queryRequestStreamId());
        } catch (final RuntimeException e) {
            aeron.close();
            throw e;
        }
    }

    /** Advances request delivery and reply publishing; call from the replica's polling thread. */
    public int poll() {
        return requests.poll(requestAssembler, FRAGMENT_LIMIT);
    }

    /** Number of queries answered. */
    public long replies() {
        return replies;
    }

    /** Number of responses dropped on publication backpressure; clients retry. */
    public long dropped() {
        return dropped;
    }

    /** Number of query requests received and decoded. */
    public long received() {
        return received;
    }

    private void onRequest(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                || headerDecoder.templateId() != QueryRequestDecoder.TEMPLATE_ID
                || length < MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
            return;
        }
        requestDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        received++;

        final int channelLength = validChannelLength();
        if (channelLength == 0) {
            return;
        }
        // The uint32 stream id narrows to a Java int; a value whose top bit is set
        // is negative and invalid for Aeron, so reject it rather than open a
        // publication on a bogus stream.
        final int responseStreamId = (int) requestDecoder.responseStreamId();
        if (responseStreamId < 0) {
            return;
        }
        final int responseLength = encodeResponse();
        if (responseLength <= 0) {
            return;
        }
        final Publication publication = responsePublication(responseStreamId, channelLength);
        if (publication == null) {
            dropped++;
            return;
        }
        final long result = publication.offer(responseBuffer, 0, responseLength);
        if (result < 0) {
            dropped++;
        } else {
            replies++;
        }
    }

    /**
     * Copies the response channel into {@link #channelScratch} and returns its
     * length, or {@code 0} when the channel is empty, oversized, or not a
     * supported scheme. No String is allocated for validation.
     */
    private int validChannelLength() {
        final int length = requestDecoder.responseChannelLength();
        if (length == 0 || length > MAX_CHANNEL_LENGTH) {
            return 0;
        }
        requestDecoder.getResponseChannel(channelScratch, 0, length);
        return hasPrefix(UDP_CHANNEL_PREFIX, length) || hasPrefix(IPC_CHANNEL_PREFIX, length) ? length : 0;
    }

    private boolean hasPrefix(final byte[] prefix, final int length) {
        if (length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (channelScratch[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** A deterministic composite key of the channel bytes and stream id. */
    private long channelKey(final int length, final int streamId) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < length; i++) {
            hash ^= channelScratch[i] & 0xFF;
            hash *= 0x100000001b3L;
        }
        return (hash << 32) ^ (streamId & 0xFFFFFFFFL);
    }

    private boolean channelMatches(final int slot, final int length) {
        final byte[] stored = publicationChannels[slot];
        if (stored == null || stored.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (stored[i] != channelScratch[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] channelBytesCopy(final int length) {
        final byte[] copy = new byte[length];
        System.arraycopy(channelScratch, 0, copy, 0, length);
        return copy;
    }

    private int encodeResponse() {
        final QueryType type = requestDecoder.queryType();
        responseEncoder
                .wrapAndApplyHeader(responseBuffer, 0, headerEncoder)
                .requestId(requestDecoder.requestId())
                .queryType(type)
                .status(QueryStatusCode.SUCCESS)
                .appliedPosition(replica.appliedPosition());
        switch (type) {
            case BALANCE -> encodeBalance();
            case USER_EXISTS -> encodeUserExists();
            case L2_ORDER_BOOK -> encodeL2();
            case SINGLE_USER_REPORT -> encodeUserReport();
            case ORDER_HISTORY -> encodeHistory(false);
            case ACTIVE_ORDERS -> encodeHistory(true);
            case ORDER_BY_ID -> encodeOrderById();
            case USER_TRADES -> encodeTrades(true);
            case MARKET_TRADES -> encodeTrades(false);
            case TOTAL_CURRENCY_BALANCE -> encodeTotals();
            case STATE_HASH -> responseEncoder.stateHash(replica.stateHash());
            default -> responseEncoder.status(QueryStatusCode.UNSUPPORTED);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
    }

    private void encodeBalance() {
        final long uid = requestDecoder.uid();
        final boolean exists = replica.userExists(uid);
        responseEncoder
                .uid(uid)
                .balance(replica.balance(uid, requestDecoder.currency()))
                .userExists(exists ? (short) 1 : (short) 0);
        if (!exists) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
        }
    }

    private void encodeUserExists() {
        final long uid = requestDecoder.uid();
        responseEncoder.uid(uid).userExists(replica.userExists(uid) ? (short) 1 : (short) 0);
    }

    private void encodeL2() {
        final int symbolId = requestDecoder.symbolId();
        final int maxLevels = requestDecoder.maxLevels() == 0
                ? l2View.maxLevels()
                : Math.min(requestDecoder.maxLevels(), l2View.maxLevels());
        responseEncoder.symbolId(symbolId);
        if (!replica.orderBook(symbolId, l2View)) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
            return;
        }
        final int askDepth = Math.min(l2View.askDepth(), maxLevels);
        final int bidDepth = Math.min(l2View.bidDepth(), maxLevels);
        final QueryResponseEncoder.AsksEncoder asks = responseEncoder.asksCount(askDepth);
        for (int i = 0; i < askDepth; i++) {
            asks.next().price(l2View.askPrice(i)).size(l2View.askVolume(i)).orders(l2View.askOrders(i));
        }
        final QueryResponseEncoder.BidsEncoder bids = responseEncoder.bidsCount(bidDepth);
        for (int i = 0; i < bidDepth; i++) {
            bids.next().price(l2View.bidPrice(i)).size(l2View.bidVolume(i)).orders(l2View.bidOrders(i));
        }
    }

    private void encodeUserReport() {
        final long uid = requestDecoder.uid();
        final SingleUserReport report = replica.singleUserReport(uid);
        responseEncoder
                .uid(uid)
                .userExists(report.exists() ? (short) 1 : (short) 0)
                .suspended(report.suspended() ? (short) 1 : (short) 0);
        if (!report.exists()) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
            return;
        }

        final int balanceCount = reportBalanceCount(report);
        final int balancesFit = fitFixedElements(
                balanceCount, QueryResponseEncoder.BalancesEncoder.sbeBlockLength(), remainingBudget());
        if (balancesFit < balanceCount) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }
        final QueryResponseEncoder.BalancesEncoder balances = responseEncoder.balancesCount(balancesFit);
        final int[] encodedBalances = {0};
        report.forEachBalance((currency, balance) -> {
            if (encodedBalances[0] < balancesFit) {
                balances.next().currency((int) currency).balance(balance);
                encodedBalances[0]++;
            }
        });

        final List<SingleUserReport.OrderLine> orders = report.orders();
        final int ordersFit =
                fitFixedElements(orders.size(), QueryResponseEncoder.OrdersEncoder.sbeBlockLength(), remainingBudget());
        if (ordersFit < orders.size()) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }
        final QueryResponseEncoder.OrdersEncoder ordersEncoder = responseEncoder.ordersCount(ordersFit);
        for (int i = 0; i < ordersFit; i++) {
            final SingleUserReport.OrderLine order = orders.get(i);
            ordersEncoder
                    .next()
                    .symbolId(order.symbolId())
                    .orderId(order.orderId())
                    .ask(order.ask() ? (short) 1 : (short) 0)
                    .price(order.price())
                    .size(order.size())
                    .filled(order.filled())
                    .reserveBidPrice(order.reserveBidPrice());
        }
    }

    private void encodeHistory(final boolean activeOnly) {
        final long uid = requestDecoder.uid();
        responseEncoder.uid(uid);
        final List<OrderRecord> records = activeOnly ? replica.activeOrders(uid) : replica.orderHistory(uid);
        if (!replica.userExists(uid) && records.isEmpty()) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
            return;
        }
        encodeHistoryRecords(records);
    }

    private void encodeOrderById() {
        final OrderRecord record = replica.order(requestDecoder.orderId());
        if (record == null) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
            return;
        }
        responseEncoder.uid(record.uid());
        encodeHistoryRecords(List.of(record));
    }

    private void encodeHistoryRecords(final List<OrderRecord> records) {
        int fit = 0;
        long budget = remainingBudget() - GROUP_SIZE_ENCODING_LENGTH;
        for (final OrderRecord record : records) {
            final long needed = historyRecordSize(record);
            if (needed > budget) {
                break;
            }
            budget -= needed;
            fit++;
        }
        if (fit < records.size()) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }
        final QueryResponseEncoder.HistoryEncoder history = responseEncoder.historyCount(fit);
        for (int i = 0; i < fit; i++) {
            encodeHistoryRecord(history.next(), records.get(i));
        }
    }

    private static void encodeHistoryRecord(
            final QueryResponseEncoder.HistoryEncoder history, final OrderRecord record) {
        history.symbolId(record.symbolId())
                .orderId(record.orderId())
                .uid(record.uid())
                .ask(record.ask() ? (short) 1 : (short) 0)
                .orderType(orderType(record.orderType()))
                .price(record.price())
                .size(record.size())
                .filled(record.filled())
                .reduced(record.reduced())
                .lastTimestamp(record.lastTimestamp())
                .placedTimestamp(record.placedTimestamp())
                .userCookie(record.userCookie())
                .state((short) record.state());
        final List<Fill> fills = record.fills();
        final QueryResponseEncoder.HistoryEncoder.FillsEncoder fillsEncoder = history.fillsCount(fills.size());
        for (final Fill fill : fills) {
            fillsEncoder
                    .next()
                    .price(fill.price())
                    .size(fill.size())
                    .taker(fill.taker() ? (short) 1 : (short) 0)
                    .counterpartyUid(fill.counterpartyUid())
                    .timestamp(fill.timestamp());
        }
    }

    private void encodeTrades(final boolean userTrades) {
        final int limit = requestDecoder.tradeLimit();
        final List<MarketTrade> trades;
        if (userTrades) {
            final long uid = requestDecoder.uid();
            responseEncoder.uid(uid);
            trades = replica.userTrades(uid, limit);
            if (!replica.userExists(uid) && trades.isEmpty()) {
                responseEncoder.status(QueryStatusCode.NOT_FOUND);
                return;
            }
        } else {
            final int symbolId = requestDecoder.symbolId();
            responseEncoder.symbolId(symbolId);
            trades = replica.marketTrades(symbolId, limit);
        }
        final int fit =
                fitFixedElements(trades.size(), QueryResponseEncoder.TradesEncoder.sbeBlockLength(), remainingBudget());
        if (fit < trades.size()) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }
        final QueryResponseEncoder.TradesEncoder encoder = responseEncoder.tradesCount(fit);
        for (int i = 0; i < fit; i++) {
            final MarketTrade trade = trades.get(i);
            encoder.next()
                    .symbolId(trade.symbolId())
                    .makerOrderId(trade.makerOrderId())
                    .makerUid(trade.makerUid())
                    .takerUid(trade.takerUid())
                    .price(trade.price())
                    .size(trade.size())
                    .timestamp(trade.timestamp());
        }
    }

    private void encodeTotals() {
        final TotalCurrencyBalance totals = replica.totalCurrencyBalance();
        final int count = totals.currencyCount();
        final int fit = fitFixedElements(count, QueryResponseEncoder.TotalsEncoder.sbeBlockLength(), remainingBudget());
        if (fit < count) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }
        final QueryResponseEncoder.TotalsEncoder encoder = responseEncoder.totalsCount(fit);
        final int[] encoded = {0};
        totals.forEach((currency, total) -> {
            if (encoded[0] < fit) {
                final int c = (int) currency;
                encoder.next()
                        .currency(c)
                        .accountBalance(totals.accountBalances(c))
                        .reserved(totals.ordersBalances(c))
                        .fees(totals.fees(c));
                encoded[0]++;
            }
        });
    }

    private static int reportBalanceCount(final SingleUserReport report) {
        final int[] count = {0};
        report.forEachBalance((currency, balance) -> count[0]++);
        return count[0];
    }

    private int remainingBudget() {
        return RESPONSE_BUFFER_CAPACITY - responseEncoder.limit();
    }

    private static int fitFixedElements(final int count, final int blockLength, final int budget) {
        final long needed = GROUP_SIZE_ENCODING_LENGTH + (long) count * blockLength;
        if (needed <= budget) {
            return count;
        }
        return Math.max(0, (budget - GROUP_SIZE_ENCODING_LENGTH) / blockLength);
    }

    private static long historyRecordSize(final OrderRecord record) {
        return QueryResponseEncoder.HistoryEncoder.sbeBlockLength()
                + GROUP_SIZE_ENCODING_LENGTH
                + (long) record.fills().size() * QueryResponseEncoder.HistoryEncoder.FillsEncoder.sbeBlockLength();
    }

    private static OrderType orderType(final String name) {
        switch (name) {
            case "GTC":
                return OrderType.GTC;
            case "IOC":
                return OrderType.IOC;
            case "FOK_BUDGET":
                return OrderType.FOK_BUDGET;
            default:
                return OrderType.NULL_VAL;
        }
    }

    // The channel bytes are still in channelScratch (single-threaded poll); the
    // String form is decoded only on a miss, when a publication must be created.
    private Publication responsePublication(final int streamId, final int channelLength) {
        final long key = channelKey(channelLength, streamId);
        for (int i = 0; i < publicationCount; i++) {
            final int index = (publicationHead + i) & RESPONSE_PUBLICATIONS_MASK;
            if (publicationKeys[index] == key && channelMatches(index, channelLength)) {
                if (i != publicationCount - 1) {
                    // Access-order LRU: move the hit to the tail so the oldest
                    // *used* entry is evicted when the cache is full.
                    final Publication publication = publicationSlots[index];
                    final long storedKey = publicationKeys[index];
                    final byte[] storedChannel = publicationChannels[index];
                    for (int j = i + 1; j < publicationCount; j++) {
                        final int src = (publicationHead + j) & RESPONSE_PUBLICATIONS_MASK;
                        final int dst = (publicationHead + j - 1) & RESPONSE_PUBLICATIONS_MASK;
                        publicationKeys[dst] = publicationKeys[src];
                        publicationChannels[dst] = publicationChannels[src];
                        publicationSlots[dst] = publicationSlots[src];
                    }
                    final int tail = (publicationHead + publicationCount - 1) & RESPONSE_PUBLICATIONS_MASK;
                    publicationKeys[tail] = storedKey;
                    publicationChannels[tail] = storedChannel;
                    publicationSlots[tail] = publication;
                }
                return publicationSlots[(publicationHead + publicationCount - 1) & RESPONSE_PUBLICATIONS_MASK];
            }
        }
        Publication publication;
        try {
            final String channel = new String(channelScratch, 0, channelLength, StandardCharsets.UTF_8);
            publication = aeron.addPublication(channel, streamId);
        } catch (final RuntimeException e) {
            // A malformed channel must not kill the poll loop; skip the reply.
            return null;
        }
        if (publicationCount == MAX_RESPONSE_PUBLICATIONS) {
            // Full: evict the oldest entry before inserting the newest.
            final int evicted = publicationHead;
            publicationSlots[evicted].close();
            publicationSlots[evicted] = null;
            publicationKeys[evicted] = 0L;
            publicationChannels[evicted] = null;
            publicationHead = (publicationHead + 1) & RESPONSE_PUBLICATIONS_MASK;
            publicationCount--;
        }
        final int slot = (publicationHead + publicationCount) & RESPONSE_PUBLICATIONS_MASK;
        publicationKeys[slot] = key;
        publicationChannels[slot] = channelBytesCopy(channelLength);
        publicationSlots[slot] = publication;
        publicationCount++;
        return publication;
    }

    @Override
    public void close() {
        for (int i = 0; i < publicationCount; i++) {
            final int index = (publicationHead + i) & RESPONSE_PUBLICATIONS_MASK;
            final Publication publication = publicationSlots[index];
            if (publication != null) {
                publication.close();
            }
        }
        publicationHead = 0;
        publicationCount = 0;
        requests.close();
        aeron.close();
    }
}
