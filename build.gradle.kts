plugins {
    java
    id("com.gradleup.shadow") version "9.6.1"
}

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

repositories { mavenCentral() }

val flink = "2.2.1"

dependencies {
    compileOnly("org.apache.flink:flink-streaming-java:$flink") // in the Flink image
    implementation("org.apache.flink:flink-connector-base:$flink")
    implementation("org.apache.flink:flink-connector-kafka:5.0.0-2.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
}

// The one jar the image runs: local:///opt/flink/usrlib/job.jar
tasks.shadowJar {
    archiveFileName = "job.jar"
    manifest { attributes["Main-Class"] = "foundry.join.Join" }
    mergeServiceFiles()
}
