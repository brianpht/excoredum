// protocol: SBE schema and generated flyweight codecs.
// Kept as a dedicated dependency-only module (no dependency on core),
// per Aeron ecosystem convention.

val generatedDir = layout.buildDirectory.dir("generated-src/sbe")

val sbeTool: Configuration by configurations.creating

dependencies {
    sbeTool(libs.sbe.tool)
    // Generated SBE codecs require Agrona at compile and runtime.
    api(libs.agrona)
}

val generateSbe by tasks.registering(JavaExec::class) {
    group = "sbe"
    description = "Generates SBE encoders/decoders from messages.xml"
    val schema = layout.projectDirectory.file("src/main/resources/messages.xml")
    inputs.file(schema)
    outputs.dir(generatedDir)

    classpath = sbeTool
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    systemProperty("sbe.output.dir", generatedDir.get().asFile.absolutePath)
    systemProperty("sbe.target.language", "Java")
    systemProperty("sbe.validation.stop.on.error", "true")
    args(schema.asFile.absolutePath)
}

sourceSets {
    main {
        java {
            srcDir(generateSbe)
        }
    }
}

// Generated code should not be subject to project lint/format gates.
tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.remove("-Werror")
    options.compilerArgs.remove("-Xlint:all")
    options.compilerArgs.add("-nowarn")
}

// Generated codecs are not written to be checkstyle-clean, so lint only the
// hand-written sources (QueryStreams.java) rather than disabling checkstyle
// module-wide and exempting them too.
tasks.named<Checkstyle>("checkstyleMain") {
    source = fileTree("src/main/java")
}

spotless {
    java {
        targetExclude("**/build/**", "**/generated-src/**")
    }
}
