# Homework 4: Design Solution for a Data Pipeline

## Scenario

The goal is to design a data pipeline for collecting and annotating direct marketing materials from different parts of the country in order to understand what types of creatives are used across different industries.

The solution is designed as an automated data pipeline, not as a full production application implementation. The focus is on requirements, data flow, service tradeoffs, cost-sensitive decisions, and a concrete final architecture.

## Scope

The project scope was intentionally narrowed to keep the design realistic for the homework:

- Input materials are single-page image documents.
- Supported formats are `JPG`, `JPEG`, and `PNG`.
- PDF documents and digital-only channels such as social media ads, banners, SMS campaigns, and landing pages are out of scope.
- Ukrainian OCR support is mandatory.
- OCR extracts text only; final creative labels are assigned manually by reviewers.
- Processing is batch or near-batch, not real-time.
- Initial scale is approximately `30,000` materials per month, with a target scale of `300,000` materials per month.

Detailed assumptions are described in [analyze_assumptions.md](./analyze_assumptions.md).

## Requirements

The functional requirements cover the main pipeline stages:

- ingestion
- raw file storage
- image pre-processing
- OCR
- metadata extraction
- human annotation
- structured storage and analysis access

See [analyze_functional.md](./analyze_functional.md).

The non-functional requirements cover:

- throughput
- latency
- availability
- scalability
- reliability
- security and compliance
- cost efficiency
- observability
- data quality

See [analyze_non_functional.md](./analyze_non_functional.md).

## Raw Architecture

The raw architecture is vendor-neutral. It uses generic component names and shows the logical data flow before choosing specific technologies.

The main flow is:

```text
Upload / Batch Source
  -> Ingestion
  -> Raw Storage
  -> Processing Queue
  -> Pre-processing
  -> OCR
  -> Metadata Extraction
  -> Structured Storage
  -> Annotation
  -> Annotated Dataset
  -> Analysis / Export
```

The raw Mermaid diagram is available in [raw_solution.md](./raw_solution.md).

## Service Analysis

The service analysis compares open-source tools, managed cloud services, SaaS products, and build-from-scratch options for the main services:

- OCR / document understanding
- annotation interface
- workflow / processing orchestration
- storage and analytical access
- identity and access control

Key conclusions:

- General image OCR is enough for the first version.
- Document AI is not required unless layout-aware extraction becomes a requirement.
- AWS Textract is not selected because Ukrainian OCR support is required.
- Google Cloud Vision OCR and Azure AI Vision OCR are stronger cloud candidates for this scope.
- Label Studio is a practical annotation choice, but it should be hosted inside the selected cloud environment and protected with identity controls.
- Airflow is useful for scheduled jobs, reprocessing, backfills, exports, and quality checks, but it is not forced into the event-driven ingestion path.

See [analyze_services.md](./analyze_services.md).

## OCR Pricing Analysis

OCR was selected for detailed pricing analysis because it is central to the pipeline, usage-based, and affected by the Ukrainian language requirement.

The recommended OCR option is Google Cloud Vision OCR for the current design because it:

- supports the image-only input scope
- supports Ukrainian OCR
- has simple usage-based pricing
- integrates well with a Google Cloud-centered stack
- is sufficient for searchable text and simple metadata extraction

See [analyze_ocr_pricing.md](./analyze_ocr_pricing.md).

## Final Solution

The final architecture is a Google Cloud-centered hybrid solution:

- Google Cloud IAM / Identity Platform for identity and access control
- Cloud Run for the custom upload API and processing workers
- Cloud Storage for raw images
- Pub/Sub for processing jobs
- Cloud Vision OCR for Ukrainian-capable image OCR
- BigQuery for analysis-ready metadata and annotations
- Label Studio for manual annotation
- Cloud SQL for Label Studio operational state
- Cloud Monitoring and Cloud Logging for observability
- Cloud Composer / Airflow as an optional component for scheduled reprocessing, exports, backfills, and data quality checks

Label Studio is the main non-native component. In the proposed design, it is self-hosted on Cloud Run or GKE and protected with Google Identity-Aware Proxy or equivalent identity controls. If strict centralized SSO/RBAC is required, Label Studio Enterprise would be a cleaner option.

See [final_solution.md](./final_solution.md).

## Document Map

| Homework Step | Document |
|---|---|
| Assumptions on scale, volume, latency, scope | [analyze_assumptions.md](./analyze_assumptions.md) |
| Functional requirements | [analyze_functional.md](./analyze_functional.md) |
| Non-functional requirements | [analyze_non_functional.md](./analyze_non_functional.md) |
| Raw vendor-neutral solution diagram | [raw_solution.md](./raw_solution.md) |
| Service option analysis | [analyze_services.md](./analyze_services.md) |
| Pricing and recommendation table for OCR | [analyze_ocr_pricing.md](./analyze_ocr_pricing.md) |
| Final concrete service diagram | [final_solution.md](./final_solution.md) |
