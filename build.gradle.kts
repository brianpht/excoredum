import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.tasks.JacocoReport
import net.ltgt.gradle.errorprone.errorprone

plugins {
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.errorprone) apply false
    alias(libs.plugins.dependencycheck)
}

// Supply-chain scan (OWASP). Runs on demand and nightly, not the fast PR gate.
// Set NVD_API_KEY to avoid NVD rate limiting; fails on High+.
dependencyCheck {
    formats = listOf("HTML", "SARIF")
    failBuildOnCVSS = 7.0f
    nvd.apiKey = System.getenv("NVD_API_KEY")
    analyzers.assemblyEnabled = false
}

val targetJava = (property("targetJavaVersion") as String).toInt()
val checkstyleVersion = libs.versions.checkstyle.get()
val jacocoVersion = libs.versions.jacoco.get()
val errorproneCoreDep = libs.errorprone.core
val nullawayDep = libs.nullaway

// Aeron/Agrona 2.x access jdk.internal.misc.Unsafe and sun.nio.ch on modern JDKs.
val aeronJvmArgs = listOf(
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
)
extra["aeronJvmArgs"] = aeronJvmArgs

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "jacoco")

    repositories {
        mavenCentral()
    }

    // Opt-in static analysis: -PwithErrorProne enables ErrorProne + NullAway,
    // report-only until the codebase is triaged clean.
    val errorProneEnabled = project.hasProperty("withErrorProne")
    if (errorProneEnabled) {
        apply(plugin = "net.ltgt.errorprone")
        dependencies {
            "errorprone"(errorproneCoreDep)
            "errorprone"(nullawayDep)
        }
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(targetJava))
        }
    }

    extensions.configure<org.gradle.api.plugins.quality.CheckstyleExtension> {
        toolVersion = checkstyleVersion
        configDirectory.set(rootProject.layout.projectDirectory.dir("config/checkstyle"))
        isIgnoreFailures = false
        maxWarnings = 0
    }

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            target("src/**/*.java")
            targetExclude("**/build/**", "**/generated/**")
            palantirJavaFormat()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-Xlint:all")
        // ErrorProne runs report-only, so -Werror is dropped while it is enabled.
        if (!errorProneEnabled) {
            options.compilerArgs.add("-Werror")
        } else {
            options.errorprone {
                disableWarningsInGeneratedCode.set(true)
                option("NullAway:AnnotatedPackages", "com.exadbe")
            }
        }
    }

    extensions.configure<JacocoPluginExtension> {
        toolVersion = jacocoVersion
    }
    tasks.withType<JacocoReport>().configureEach {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs(aeronJvmArgs)
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
    }

    // Generated SBE codecs do not carry lint-clean Javadoc; disable doclint.
    tasks.withType<Javadoc>().configureEach {
        options.encoding = "UTF-8"
        (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
    }
}
