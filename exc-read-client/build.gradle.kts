// exc-read-client: the read-side SDK. Consumes ONLY the exc-protocol wire
// contract (QueryRequest / QueryResponse) and deliberately does NOT depend on
// exc-core or exc-read, mirroring how exc-client stays decoupled from the
// engine. It queries a running read replica's QueryResponder over plain Aeron
// request/response streams with request-id correlation and idempotent retry.

dependencies {
    api(project(":exc-protocol"))
    api(libs.aeron.client)
    api(libs.agrona)
    implementation(libs.aeron.driver)
}
