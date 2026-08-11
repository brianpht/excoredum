rootProject.name = "excoredum"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("exc-protocol")
include("exc-core")
include("exc-launcher")
include("exc-client")
include("exc-read")
include("exc-gateway-rest")
include("exc-tests")
include("exc-examples")
include("exc-bench")
include("exc-xcore-bench")
