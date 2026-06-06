# Spool — Infrastructure

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Infrastructure** is the **Spool** framework module that provides **ready-to-use adapters** for connecting the framework to real external systems — HTTP sources, file system, S3, Kafka, and normalizers for JSON, YAML, PDF, and images.

---

## Table of Contents

- [What it provides](#what-it-provides)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [HTTP Poll Source](#http-poll-source)
  - [File System Inbox](#file-system-inbox)
  - [S3 Inbox](#s3-inbox)
  - [Custom Plugin via SPI](#custom-plugin-via-spi)
- [Available Adapters](#available-adapters)
- [Local Development Stack](#local-development-stack)
- [License](#license)

---

## What it provides

```
spool-infrastructure
│
├── adapter/pollsource/    HTTPPollSource
├── adapter/inbox/         FileSystemInboxWriter, S3InboxWriter
├── adapter/datalake/      FileSystemDataLakeWriter, S3DataLakeWriter
├── adapter/datamart/      FileSystemDataMartWriter
├── adapter/serde/         JSON, JSON_ARRAY, YAML, PDF, Image, EventClass normalizers
├── adapter/bus/           Kafka EventBus
└── spi/                   Plugin, PluginRegistry, PluginResolver
```

All adapters are registered via the `@SpoolPlugin` annotation and discovered at runtime through `PluginRegistry`.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>infrastructure</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 21+**.

---

## Quick Start

Most adapters can be instantiated directly without any plugin resolution.

```java
// HTTP source — fetches JSON from a REST endpoint
PollSource<byte[]> source = new HTTPPollSource("https://api.example.com/trades", "gemini-btcusd");

// Build the crawler with the HTTP source and JSON_ARRAY normalizer
Crawler crawler = CrawlerBuilderFactory.poll(source)
        .source()
            .ports(CrawlerPorts.builder()
                    .inbox(new FileSystemInboxWriter(Path.of("/var/spool/inbox")))
                    .bus(eventBus)
                    .build())
            .and()
        .createWith(StandardNormalizer.JSON_ARRAY);
```

---

## Examples

### HTTP Poll Source

`HTTPPollSource` makes a GET request and returns the response as `byte[]`.

```java
PollSource<byte[]> source = new HTTPPollSource(
        "https://api.example.com/records",
        "my-source-id"           // used as sourceId and in error messages
);
```

The source sets a 5-second connect timeout and a 10-second read timeout and expects HTTP 200.

---

### File System Inbox

Write and read envelopes as files on a local directory.

```java
FileSystemInboxWriter writer = new FileSystemInboxWriter(Path.of("/var/spool/inbox"));

CrawlerPorts ports = CrawlerPorts.builder()
        .inbox(writer)
        .bus(eventBus)
        .build();
```

---

### S3 Inbox

Use S3 (or LocalStack for local development) as the inbox backend.

```java
S3InboxWriter writer = new S3InboxWriter(s3Client, "my-inbox-bucket");

CrawlerPorts ports = CrawlerPorts.builder()
        .inbox(writer)
        .bus(eventBus)
        .build();
```

---

### Custom Plugin via SPI

Implement the `Plugin<T>` interface to create a custom adapter and register it for discovery.

```java
@SpoolPlugin(InboxWriterProvider.class)
public class MyInboxWriterProvider implements InboxWriterProvider {

    @Override
    public String name() { return "my-inbox"; }

    @Override
    public int priority() { return 10; }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.has("myHost");
    }

    @Override
    public InboxWriter create(PluginConfiguration configuration) {
        String host = configuration.require("myHost");
        return (envelope) -> myStorage.save(envelope);
    }
}
```

Register in `META-INF/services/software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider`:

```
com.example.MyInboxWriterProvider
```

---

## Available Adapters

| Category | Adapter | Description |
|---|---|---|
| Poll Source | `HTTPPollSource` | GET request, returns `byte[]`. |
| Inbox | `FileSystemInboxWriter` | Writes envelopes as files. |
| Inbox | `S3InboxWriter` | Writes envelopes to an S3 bucket. |
| Data Lake | `FileSystemDataLakeWriter` | Writes batches as files. |
| Data Lake | `S3DataLakeWriter` | Writes batches to an S3 bucket. |
| Data Mart | `FileSystemDataMartWriter` | Writes mount results as files. |
| Normalizer | `JSONNormalizerProvider` | Parses a single JSON object. |
| Normalizer | `JSONArrayNormalizerProvider` | Parses a JSON array and splits elements. |
| Normalizer | `PDFNormalizerProvider` | Extracts text from PDF documents. |
| Normalizer | `ImageNormalizerProvider` | Processes image payloads. |
| Normalizer | `EventClassNormalizerProvider` | Deserializes directly to a typed event class. |
| Event Bus | Kafka adapter | Kafka-backed `EventBus` implementation. |

---

## Local Development Stack

The `.docker` folder contains a `docker-compose.yml` with Kafka and LocalStack (S3) pre-configured for local development.

```bash
cd .docker
docker compose up -d
```

| Service | Port | Purpose |
|---|---|---|
| Kafka | `9092` | Event bus |
| LocalStack (S3) | `4566` | Inbox and data lake storage |

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
