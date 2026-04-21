plugins {
    java
    distribution
}

group = "com.starrocks"
version = "1.0.8"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven {
        name = "Confluent"
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    // Import BOMs to align versions
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.21.2"))
    implementation(platform("io.debezium:debezium-bom:3.5.0.Final"))

    // Kafka Connect dependencies
    compileOnly("org.apache.kafka:connect-api")
    compileOnly("org.apache.kafka:connect-transforms")
    // implementation("org.apache.kafka:connect-runtime")

    // Debezium dependencies
    implementation("io.debezium:debezium-connect-plugins")
    implementation("io.debezium:debezium-connector-common")

    // Jackson dependencies
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-annotations")

    compileOnly("ch.qos.logback:logback-classic:1.5.32")
    compileOnly("io.confluent:kafka-connect-avro-converter:8.2.0") {
        exclude(group = "org.apache.kafka", module = "*")
    }

    // Other dependencies
    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("com.starrocks:starrocks-stream-load-sdk:1.0")

    // Test dependencies
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.slf4j:slf4j-log4j12:1.7.25")
}

tasks.test {
    useJUnitPlatform()
}

distributions {
    main {
        distributionBaseName = "starrocks-connector-for-kafka"
        contents {
            from(tasks.jar)
            from(configurations.runtimeClasspath)
            exclude("slf4j-*.jar")
            exclude("commons-logging-*.jar")
            exclude("kafka-clients-*.jar")
            exclude("connect-api-*.jar")
            exclude("jackson-*.jar")
            exclude("jakarta.*.jar")
            exclude("lz4-*.jar")
            exclude("zstd-*.jar")
            exclude("snappy-*.jar")
        }
    }
}
