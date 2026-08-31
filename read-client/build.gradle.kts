// read-client: the read-side SDK. Consumes ONLY the protocol wire
// contract (QueryRequest / QueryResponse) and deliberately does NOT depend on
// core or read, mirroring how write-client stays decoupled from the
// engine. It queries a running read replica's QueryResponder over plain Aeron
// request/response streams with request-id correlation and idempotent retry.

dependencies {
    api(project(":protocol"))
    api(libs.aeron.client)
    api(libs.agrona)
    implementation(libs.aeron.driver)
}
