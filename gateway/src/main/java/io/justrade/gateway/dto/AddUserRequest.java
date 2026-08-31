package io.justrade.gateway.dto;

/** JSON body for the admin {@code POST /api/v1/users} endpoint. */
public record AddUserRequest(Long uid) {}
