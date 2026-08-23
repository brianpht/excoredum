package com.exadbe.gateway.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared Jackson {@link ObjectMapper} for the HTTP boundary. The mapper is
 * configured once and read-only thereafter, so it is thread-safe for
 * serialization across Netty event loops.
 */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String write(final Object value) throws JsonProcessingException {
        return MAPPER.writeValueAsString(value);
    }
}
