plugins {
    id("java")
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.0"
}

group = "org.example"
version = "v1"

description = "Flink Quickstart Job"

val flinkVersion = "1.17.2"
val slf4jVersion = "2.1.0-alpha1"
val log4jVersion = "2.17.1"
val cassandra = "3.2.0-1.19"
val kafkaConnector = "1.17.2"
val kafka = "3.8.0"
val opencvRelatedVersionsPackages = "4.9.0-1.5.10"
val boof= "1.1.5"

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots")
    }
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-cassandra_2.12:$cassandra")
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnector")
    implementation("org.apache.kafka:kafka_2.13:$kafka")
    implementation("org.apache.opennlp:opennlp-tools:2.4.0")
    implementation("org.apache.commons:commons-configuration2:2.11.0")
    implementation("org.ini4j:ini4j:0.5.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("org.apache.flink:flink-connector-files:1.20.0")
    implementation("com.github.oshi:oshi-core:6.1.5")
    implementation("net.java.dev.jna:jna:5.10.0")
    implementation("net.java.dev.jna:jna-platform:5.10.0")
    implementation("org.apache.flink:flink-metrics-slf4j:1.20.0")
    implementation("org.apache.flink:flink-connector-base:1.20.0")
    // https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-dropwizard
    implementation("org.apache.flink:flink-metrics-dropwizard:1.20.0")

//    implementation("org.apache.flink:flink-shaded-guava:31.1-jre-17.0")

    //---------------------------------------------------------------
    implementation("org.slf4j:slf4j-api:2.1.0-alpha1")
    implementation("org.slf4j:slf4j-simple:2.1.0-alpha1")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.example.Consumer"
    }
    from({
        configurations.runtimeClasspath.get().filter { it.isDirectory }.map { zipTree(it) }
    })
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

application {
    mainClass.set("org.example.Consumer")
}

tasks.named("build") {
    dependsOn("shadowJar")
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveBaseName.set("Consumer")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
    isZip64 = true
}

tasks.named<JavaExec>("run") {
    jvmArgs(
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--illegal-access=permit",

        )
}

tasks.named<Test>("test") {
    jvmArgs(
        "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED"
    )
}