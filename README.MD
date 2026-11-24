# Spool — Infrastructure

[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**Infrastructure** is the **Spool** framework module that provides **ready-to-use adapters** for connecting the framework to real external systems: HTTP sources, file system, S3, Kafka, normalizers for JSON and PDF, and request authentication. The full list is in [Available Adapters](#available-adapters).

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
├── adapter/serde/         JSON, JSON_ARRAY, JSON_OBJECT, PDF and EventClass normalizers
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

When the source answers 429 or 503 saying how long to wait, `fetch` throws `PieceAsksToWaitException` with the time
to call again, which the monitor of the piece uses to open its circuit until then. By default the wait comes from the
`Retry-After` header, in seconds or as an HTTP date, and is cut to one hour. A `RetryAfter` says where else it comes
from (a header or a field of the JSON body), in which format and the longest wait accepted:

```java
PollSource<byte[]> source = new HTTPPollSource("https://api.example.com/records", "my-source-id",
        new RetryAfter(new RetryAfterLocation.Header("X-RateLimit-Reset"), RetryAfterFormat.EPOCH_SECONDS,
                Duration.ofMinutes(10)),
        Clock.systemUTC());
```

The `HTTP` plugin reads the same from the optional keys `retryAfterHeader`, `retryAfterField`, `retryAfterFormat` and
`maxRetryAfter`.

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

Every adapter is a plugin: the descriptor names it by its type, and the port decides what it is for. This table is
the catalogue. `ReadmeAdaptersTest` checks that every port is in it and that each row lists exactly the types
registered for that port, so a plugin cannot be added without appearing here.

| Port | Provider | Types | Configuration |
|---|---|---|---|
| Event bus | `EventBusProvider` | `IN_MEMORY`, `KAFKA` | `KAFKA`: `bootstrap.servers`, `subscriber.id` optional |
| Inbox, write | `InboxWriterProvider` | `FILE_SYSTEM`, `S3` | `FILE_SYSTEM`: `path`. `S3`: `bucket`, `region`, `endpoint` |
| Inbox, read | `InboxReaderProvider` | `FILE_SYSTEM`, `S3` | same as the inbox writer |
| Inbox, update | `InboxUpdaterProvider` | `FILE_SYSTEM`, `S3` | same as the inbox writer |
| Inbox, remove | `InboxEnvelopeRemoverProvider` | `FILE_SYSTEM`, `S3` | same as the inbox writer |
| Data lake, write | `DataLakeWriterProvider` | `FILE_SYSTEM`, `S3` | `FILE_SYSTEM`: `path`. `S3`: `bucket`, `region`, `endpoint` |
| Data lake, remove | `DataLakeEnvelopeRemoverProvider` | `FILE_SYSTEM`, `S3` | same as the data lake writer |
| Data lake, read partitions | `PartitionedReaderProvider` | `FILE_SYSTEM`, `S3` | `FILE_SYSTEM`: `path`, `payload`. `S3`: same as the data lake writer |
| Envelope headers, write | `EnvelopeHeaderWriterProvider` | `POSTGRESQL` | `url` (a `jdbc:postgresql:` URL), `user`, `password`, `table` (`envelope_headers` by default), `poolSize` (4 by default) |
| Envelope headers, read | `EnvelopeHeaderReaderProvider` | `POSTGRESQL` | same as the envelope headers writer |
| Envelope headers, remove | `EnvelopeHeaderRemoverProvider` | `POSTGRESQL` | same as the envelope headers writer |
| Data mart, write | `DataMartWriterProvider` | `FILE_SYSTEM`, `RAW_FILE_SYSTEM`, `S3` | `FILE_SYSTEM` and `RAW_FILE_SYSTEM`: `path`. `S3`: `bucket`, `region`, `endpoint` |
| Quarantine | `QuarantineRepositoryProvider` | `FILE_SYSTEM` | `path` |
| Poll source | `PollSourceProvider` | `HTTP` | `sourceId`, `url`; optional `retryAfterHeader`, `retryAfterField`, `retryAfterFormat`, `maxRetryAfter` |
| Stream source | `StreamSourceProvider` | `IN_MEMORY` | `sourceId`, `eventClassName` |
| Normalizer (media type) | `NormalizerProvider` | `JSON`, `JSON_ARRAY`, `JSON_OBJECT`, `EVENT_CLASS`, `EVENT_CLASS_ARRAY`, `PDF` | `rootPath`, `rules`, set by the crawler |
| Error router | `ErrorRouterProvider` | `DEFAULT` | none |
| Terraform of a piece | `ProvisioningProvider` | `IN_MEMORY`, `FILE_SYSTEM`, `RAW_FILE_SYSTEM`, `S3`, `KAFKA`, `POSTGRESQL` | the configuration of the piece: see below |
| Health check of a piece | `HealthProbeProvider` | `FILE_SYSTEM`, `RAW_FILE_SYSTEM`, `S3`, `KAFKA`, `POSTGRESQL` | the configuration of the piece |
| Processed keys of an inbox | `ProcessedKeyRepositoryProvider` | `FILE_SYSTEM`, `S3` | the configuration of the inbox piece |
| Authentication | `RequestAuthenticatorProvider` | `OIDC`, `HMAC_SIGNATURE` | `OIDC`: `issuer`, `audience`, `jwksUri` and `rolesClaim` optional. `HMAC_SIGNATURE`: `header`, `secret`, `timestampHeader`, `tolerance` and `roles` optional |

The `S3` adapters also read `accessKeyEnv` and `secretKeyEnv`, the names of the environment variables with the
credentials.

A `ProvisioningProvider` generates the Terraform of the pieces of its type for `spool-provisioning`
([ADR-024](https://linear.app/javier-castilla/document/adr-024-cada-plugin-genera-el-terraform-de-sus-piezas-para-un-destino-e4f6ee200935)):

| Type | Local target | AWS target |
|---|---|---|
| `IN_MEMORY` | Nothing | Nothing |
| `FILE_SYSTEM`, `RAW_FILE_SYSTEM` | Nothing | Cannot be generated |
| `S3` | A bucket in a LocalStack container that every S3 piece shares, on the port of `endpoint` | An encrypted bucket closed to the public, and object access for the modules that use it |
| `KAFKA` | A broker in a container, on the port of the first of `bootstrap.servers` | Cannot be generated yet |
| `POSTGRESQL` | A database in a container of its own, on the port and with the database of `url`, for `user`; `password` has to be `${env:NAME}` | Cannot be generated yet |

A `HealthProbeProvider` builds the check of the pieces of its type, used in the health of the modules
([ADR-030](https://linear.app/javier-castilla/document/adr-030-la-salud-de-un-modulo-sale-de-cada-pieza-de-infraestructura-abe3aade918e)):
`FILE_SYSTEM` and `RAW_FILE_SYSTEM` write and delete an empty file in the folder, `S3` lists a single key of the
bucket, `KAFKA` asks the brokers for the id of their cluster and `POSTGRESQL` reads a single row of the table, with
the pool of connections of its store and a limit of 5 seconds. None of them changes what the piece keeps.

The inbox, the data lake and the data mart are files or S3 objects. The only relational backend is the `POSTGRESQL`
store of envelope headers
([ADR-023](https://linear.app/javier-castilla/document/adr-023-el-data-lake-guarda-el-payload-tal-cual-y-los-metadatos-del-735a53af507b)):
it creates its table at startup if it is missing, and the writer, the reader and the remover of the same
configuration share one store and one pool of connections. Any other database is a new adapter, written as a plugin of
the port it serves (see [Custom Plugin via SPI](#custom-plugin-via-spi)).

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

Distributed under the [GNU Affero General Public License v3.0](LICENSE).
