package com.exadbe.protocol;

/**
 * Default channels and stream ids of the read-side query protocol. Both the
 * read replica's {@code QueryResponder} and the {@code exc-read-client} SDK
 * default to these values so they agree out of the box; either side may
 * override them through its own configuration.
 */
public final class QueryStreams {

    /** Default channel a read replica listens on for {@link QueryRequest} frames. */
    public static final String QUERY_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:44000";

    /** Default stream id for {@link QueryRequest} frames. */
    public static final int QUERY_REQUEST_STREAM_ID = 300;

    /** Default stream id a client listens on for {@link QueryResponse} frames. */
    public static final int QUERY_RESPONSE_STREAM_ID = 301;

    private QueryStreams() {}
}
