package com.exadbe.engine;

import com.exadbe.protocol.CommandEnvelopeDecoder;
import com.exadbe.protocol.CommandEnvelopeEncoder;
import com.exadbe.protocol.MessageHeaderDecoder;
import com.exadbe.protocol.MessageHeaderEncoder;
import com.exadbe.protocol.OrderAction;
import com.exadbe.protocol.OrderCommandType;
import com.exadbe.protocol.OrderType;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes commands into a scratch buffer and returns a wrapped decoder, so pure
 * engine tests can drive {@link MatchingEngine} without any Aeron dependency.
 */
final class Commands {

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder envelopeDecoder = new CommandEnvelopeDecoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    CommandEnvelopeDecoder addUser(final long clientId, final long clientSeq, final long commandId, final long uid) {
        return encode(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.ADD_USER,
                uid,
                CommandEnvelopeEncoder.currencyNullValue(),
                CommandEnvelopeEncoder.balanceAmountNullValue());
    }

    CommandEnvelopeDecoder adjust(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final long uid,
            final int currency,
            final long amount) {
        return encode(clientId, clientSeq, commandId, OrderCommandType.BALANCE_ADJUSTMENT, uid, currency, amount);
    }

    CommandEnvelopeDecoder suspend(final long clientId, final long clientSeq, final long commandId, final long uid) {
        return encode(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.SUSPEND_USER,
                uid,
                CommandEnvelopeEncoder.currencyNullValue(),
                CommandEnvelopeEncoder.balanceAmountNullValue());
    }

    CommandEnvelopeDecoder resume(final long clientId, final long clientSeq, final long commandId, final long uid) {
        return encode(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.RESUME_USER,
                uid,
                CommandEnvelopeEncoder.currencyNullValue(),
                CommandEnvelopeEncoder.balanceAmountNullValue());
    }

    CommandEnvelopeDecoder addSymbol(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK) {
        return addSymbol(
                clientId, clientSeq, commandId, symbolId, baseCurrency, quoteCurrency, baseScaleK, quoteScaleK, 0L, 0L);
    }

    CommandEnvelopeDecoder addSymbol(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final int baseCurrency,
            final int quoteCurrency,
            final long baseScaleK,
            final long quoteScaleK,
            final long takerFee,
            final long makerFee) {
        envelopeEncoder
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .commandIdHi(clientId)
                .commandIdLo(commandId)
                .commandType(OrderCommandType.ADD_SYMBOL)
                .uid(0L)
                .symbolId(symbolId)
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(baseCurrency)
                .quoteCurrency(quoteCurrency)
                .baseScaleK(baseScaleK)
                .quoteScaleK(quoteScaleK)
                .takerFee(takerFee)
                .makerFee(makerFee);

        headerDecoder.wrap(buffer, 0);
        envelopeDecoder.wrap(
                buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return envelopeDecoder;
    }

    CommandEnvelopeDecoder encode(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final OrderCommandType type,
            final long uid,
            final int currency,
            final long balanceAmount) {
        envelopeEncoder
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .commandIdHi(clientId)
                .commandIdLo(commandId)
                .commandType(type)
                .uid(uid)
                .symbolId(CommandEnvelopeEncoder.symbolIdNullValue())
                .orderId(CommandEnvelopeEncoder.orderIdNullValue())
                .price(CommandEnvelopeEncoder.priceNullValue())
                .reserveBidPrice(CommandEnvelopeEncoder.reserveBidPriceNullValue())
                .size(CommandEnvelopeEncoder.sizeNullValue())
                .action(OrderAction.NULL_VAL)
                .orderType(OrderType.NULL_VAL)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(currency)
                .balanceAmount(balanceAmount)
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());

        headerDecoder.wrap(buffer, 0);
        envelopeDecoder.wrap(
                buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return envelopeDecoder;
    }

    CommandEnvelopeDecoder placeGtc(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long reserveBidPrice,
            final long uid) {
        return order(
                clientId,
                clientSeq,
                commandId,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.GTC,
                price,
                reserveBidPrice,
                size);
    }

    CommandEnvelopeDecoder placeIoc(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long price,
            final long size,
            final long uid) {
        return order(
                clientId,
                clientSeq,
                commandId,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.IOC,
                price,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size);
    }

    CommandEnvelopeDecoder placeFokBudget(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final boolean ask,
            final long budget,
            final long size,
            final long uid) {
        return order(
                clientId,
                clientSeq,
                commandId,
                symbolId,
                orderId,
                uid,
                ask ? OrderAction.ASK : OrderAction.BID,
                OrderType.FOK_BUDGET,
                budget,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                size);
    }

    CommandEnvelopeDecoder cancel(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final long uid) {
        return orderTyped(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.CANCEL_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue());
    }

    CommandEnvelopeDecoder move(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final long newPrice,
            final long uid) {
        return orderTyped(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.MOVE_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                newPrice,
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue());
    }

    CommandEnvelopeDecoder reduce(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final long reduceSize,
            final long uid) {
        return orderTyped(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.REDUCE_ORDER,
                symbolId,
                orderId,
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                reduceSize);
    }

    CommandEnvelopeDecoder orderBookRequest(
            final long clientId, final long clientSeq, final long commandId, final int symbolId, final long uid) {
        return orderTyped(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.ORDER_BOOK_REQUEST,
                symbolId,
                CommandEnvelopeEncoder.orderIdNullValue(),
                uid,
                OrderAction.NULL_VAL,
                OrderType.NULL_VAL,
                CommandEnvelopeEncoder.priceNullValue(),
                CommandEnvelopeEncoder.reserveBidPriceNullValue(),
                CommandEnvelopeEncoder.sizeNullValue());
    }

    private CommandEnvelopeDecoder order(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final int symbolId,
            final long orderId,
            final long uid,
            final OrderAction action,
            final OrderType orderType,
            final long price,
            final long reserveBidPrice,
            final long size) {
        return orderTyped(
                clientId,
                clientSeq,
                commandId,
                OrderCommandType.PLACE_ORDER,
                symbolId,
                orderId,
                uid,
                action,
                orderType,
                price,
                reserveBidPrice,
                size);
    }

    private CommandEnvelopeDecoder orderTyped(
            final long clientId,
            final long clientSeq,
            final long commandId,
            final OrderCommandType type,
            final int symbolId,
            final long orderId,
            final long uid,
            final OrderAction action,
            final OrderType orderType,
            final long price,
            final long reserveBidPrice,
            final long size) {
        envelopeEncoder
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .commandIdHi(clientId)
                .commandIdLo(commandId)
                .commandType(type)
                .uid(uid)
                .symbolId(symbolId)
                .orderId(orderId)
                .price(price)
                .reserveBidPrice(reserveBidPrice)
                .size(size)
                .action(action)
                .orderType(orderType)
                .userCookie(CommandEnvelopeEncoder.userCookieNullValue())
                .currency(CommandEnvelopeEncoder.currencyNullValue())
                .balanceAmount(CommandEnvelopeEncoder.balanceAmountNullValue())
                .baseCurrency(CommandEnvelopeEncoder.baseCurrencyNullValue())
                .quoteCurrency(CommandEnvelopeEncoder.quoteCurrencyNullValue())
                .baseScaleK(CommandEnvelopeEncoder.baseScaleKNullValue())
                .quoteScaleK(CommandEnvelopeEncoder.quoteScaleKNullValue())
                .takerFee(CommandEnvelopeEncoder.takerFeeNullValue())
                .makerFee(CommandEnvelopeEncoder.makerFeeNullValue());

        headerDecoder.wrap(buffer, 0);
        envelopeDecoder.wrap(
                buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return envelopeDecoder;
    }
}
