rootProject.name = "justrade"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("protocol")
include("core")
include("launcher")
include("write-client")
include("read")
include("read-client")
include("gateway")
include("tests")
include("examples")
include("bench")
include("xcore-bench")
