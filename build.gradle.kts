plugins {
    java
}

group = "com.starrocks"
version = "1.0.5"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Import BOMs to align versions
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.21.2"))
    implementation(platform("io.debezium:debezium-bom:3.5.0.Final"))

    // Kafka Connect dependencies
    compileOnly("org.apache.kafka:connect-api")
    compileOnly("org.apache.kafka:connect-transforms")

    // Debezium dependencies
    implementation("io.debezium:debezium-connect-plugins")
    implementation("io.debezium:debezium-connector-common")

    // Jackson dependencies
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-annotations")

    // Other dependencies
    implementation("org.slf4j:slf4j-api:1.7.25")
    implementation("com.starrocks:starrocks-stream-load-sdk:1.0")

    // Test dependencies
    testImplementation("junit:junit:4.12")
    testImplementation("org.slf4j:slf4j-log4j12:1.7.25")
}

tasks.test {
    useJUnit()
}
