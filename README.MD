# SPOOL · Infrastructure

<p align="center">
  <img src="doc/spool_logo.svg" alt="SPOOL" width="240" />
</p>

This module contains the infrastructure layer for **SPOOL**.
It is the place where framework adapters and plugins live: integration points that connect the core of SPOOL with external technologies and runtime-specific implementations.

## What is this module for?

Use this module when you need infrastructure-level building blocks for the framework, such as:

- adapters for external systems
- pluggable implementations for runtime concerns
- environment-specific integrations
- lightweight local or test-friendly alternatives

In short: this is where SPOOL keeps infrastructure plugins and adapters, not domain logic.

## Quick Start

### 1. Build the module

```bash
mvn clean package
```

### 2. Use it in your project

Add the module as a dependency and resolve the plugin you need through the registry.

### 3. Select an implementation

Choose the infrastructure plugin that fits your environment, for example a local in-memory option for development or a technology-backed adapter for real deployments.

## Requirements

- Java 17
- Maven

## Build and release

Standard build:

```bash
mvn clean package
```

Release build:

```bash
mvn -Prelease clean verify
```

## Project scope

This module is intentionally focused on infrastructure concerns only.
It is expected to host future adapters, providers, and integration plugins as SPOOL grows.

## License

Apache License 2.0
