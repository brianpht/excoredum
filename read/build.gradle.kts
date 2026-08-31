// read: the CQRS read-side. Runs a non-voting read replica that replicates
// cluster state via Aeron Archive (snapshot load + consensus log following) and
// serves eventually-consistent L2 / account reads. It never joins Raft and never
// affects quorum.

plugins {
    application
}

application {
    mainClass.set("io.justrade.read.ReadServiceLauncher")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":core"))
    implementation(libs.bundles.aeron)
}
