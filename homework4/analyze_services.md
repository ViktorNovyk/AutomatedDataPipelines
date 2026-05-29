# Analyze Service Options

This section compares implementation options for the main services in the raw solution. The focus is on tradeoffs between open-source tools, cloud provider managed services, specialized SaaS products, and building custom components from scratch.

The analyzed services are:

- OCR / document understanding
- Annotation interface
- Workflow / processing orchestration
- Storage and analytical access
- Identity and access control

## 1. OCR / Document Understanding

OCR is one of the most important services in the pipeline because most uploaded marketing materials contain text that should be searchable and useful for metadata extraction. Ukrainian language support is a mandatory requirement, so OCR services should not be evaluated only by integration convenience or price.

For the current scope, general image OCR is enough because the system processes single-page image files and needs searchable text plus simple metadata extraction. More advanced document AI services can be considered later if the requirements expand to layout-aware extraction, form parsing, table extraction, or richer document structure understanding.

Cloud providers often separate these capabilities. Google has Cloud Vision OCR for general image OCR and Document AI for document understanding. Azure has AI Vision OCR / Read-style capabilities and Azure AI Document Intelligence. AWS has image text detection options and Textract for document extraction, but Textract is not a good fit for this project because Ukrainian OCR support is required.

| Option | Examples | What You Get Out of the Box | What Still Needs to Be Built | Operational Burden | Lock-in Risk |
|---|---|---|---|---|---|
| Open-source library | Tesseract, EasyOCR, PaddleOCR | Local OCR processing, no per-page cloud cost, full control over runtime and data | Image preparation, scaling workers, quality tuning, confidence handling, monitoring | Medium to high | Low |
| Cloud provider managed service | Google Cloud Vision OCR, Azure AI Vision OCR / Read | Managed image OCR, scalable processing, text locations, confidence signals, stronger fit for multilingual OCR | Integration with pipeline, result normalization, cost controls, error handling, validation on Ukrainian marketing images | Low to medium | Medium to high |
| Cloud provider document AI | Google Document AI, Azure AI Document Intelligence | Layout-aware document extraction, structure detection, key-value or table extraction, document processing workflows | Same pipeline integration work plus processor selection, schema mapping, and validation that advanced extraction is actually needed | Medium | Medium to high |
| Specialized SaaS | ABBYY Vantage, Rossum, Nanonets, Docparser | Higher-level document processing workflows, APIs, support, possible prebuilt extraction templates | Integration, data mapping, export logic, vendor review | Low | High |
| Build from scratch | Custom OCR model | Full control and domain-specific optimization | Dataset collection, model training, serving, monitoring, retraining, quality evaluation | Very high | Low, but high internal dependency |

**AWS Textract note:** AWS Textract is not recommended for this project because its OCR language support does not cover Ukrainian. It could be considered only if the input materials were known to be in languages supported by Textract, but that conflicts with the current assumptions.

**Recommendation:** Use a managed general image OCR service with Ukrainian language support from the selected primary cloud platform when possible. Google Cloud Vision OCR or Azure AI Vision OCR / Read-style services are better first candidates than document AI services for the current scope. Google Document AI or Azure AI Document Intelligence can be considered later if the project needs layout-aware extraction. ABBYY Vantage is a strong specialized OCR option if OCR quality and language coverage are more important than cloud-native integration, but it introduces an additional external vendor. Open-source OCR is a reasonable lower-cost alternative if the team accepts more tuning and operational work.

## 2. Annotation Interface

Annotation is required because the final labels for creative format, creative intent, and industry are assigned manually by reviewers.

| Option | Examples | What You Get Out of the Box | What Still Needs to Be Built | Operational Burden | Lock-in Risk |
|---|---|---|---|---|---|
| Open-source tool | Label Studio, CVAT | Ready annotation UI, configurable labels, export formats, self-hosting option | Deployment, authentication, connection to raw files and metadata storage, operational database, reviewer workflow setup | Medium | Low to medium |
| Cloud provider managed service | SageMaker Ground Truth, cloud ML labeling tools | Managed labeling workflows, integration with cloud storage, quality control features, optional workforce integration | Pipeline integration, label schema setup, cost governance | Low to medium | Medium to high |
| Specialized SaaS | Labelbox, Scale AI, SuperAnnotate | Mature labeling workflows, collaboration, review workflows, quality management, workforce options | Integration, export mapping, vendor governance | Low | High |
| Build from scratch | Custom annotation web app | Exact fit for internal workflow | UI, authentication, task assignment, review states, export, audit trail, maintenance | High | Low, but high internal maintenance |

**Recommendation:** Use an open-source annotation tool, such as Label Studio, hosted inside the same primary cloud environment as storage and processing. It provides the needed human labeling workflow without the cost and lock-in of a specialized labeling SaaS, while still allowing integration with the same object storage, monitoring, and identity model. Label Studio should use a managed operational database for its own application state, while final reviewed labels should be exported or synchronized into the analysis-ready structured storage. A managed labeling service can make sense if annotation volume grows or external reviewer management becomes important, but it should be selected from the same cloud ecosystem where possible.

## 3. Workflow / Processing Orchestration

The pipeline needs to coordinate ingestion, pre-processing, OCR, metadata extraction, status tracking, and retries. This is the section most closely connected to automated data pipelines.

| Option | Examples | What You Get Out of the Box | What Still Needs to Be Built | Operational Burden | Lock-in Risk |
|---|---|---|---|---|---|
| Open-source orchestrator | Airflow, Prefect, Dagster | Scheduling, dependencies, retries, execution history, pipeline visibility | Deployment, worker scaling, integrations, monitoring, secrets management | Medium to high | Low to medium |
| Cloud provider managed workflow | Managed Airflow, Step Functions, Cloud Composer, Workflows, Azure Data Factory | Managed scheduling/orchestration, retries, execution history, integration with cloud services | Pipeline definitions, service integrations, cost controls | Low to medium | Medium to high |
| Specialized managed orchestration SaaS | Astronomer, Prefect Cloud, Dagster Cloud | Managed orchestration platform, UI, monitoring, collaboration features | Pipeline code, integrations, deployment configuration | Low to medium | Medium |
| Build simple queue and workers | Message queue, worker processes, status table | Simple event-driven processing, independent scaling of OCR workers, lower conceptual overhead for near-batch processing | Retry logic, observability, job state management, scheduling/backfills if needed | Medium | Low to medium |

**Recommendation:** Use a processing queue with workers for ingestion-triggered document processing, preferably from the same primary cloud platform as storage and OCR. Use an orchestrator such as Airflow only for scheduled jobs, reprocessing, backfills, or exports. This avoids forcing Airflow into simple event-driven work while still recognizing its value for batch data pipelines.

## 4. Storage and Analytical Access

The system needs two kinds of storage: raw file storage for original images and structured storage for OCR text, metadata, labels, and processing status.

| Option | Examples | What You Get Out of the Box | What Still Needs to Be Built | Operational Burden | Lock-in Risk |
|---|---|---|---|---|---|
| Open-source / self-hosted | MinIO for files, PostgreSQL for metadata | Control over data and deployment, familiar APIs, low software cost | Backups, scaling, high availability, security hardening, monitoring | Medium to high | Low |
| Cloud provider managed storage | Object storage, managed relational database, managed warehouse | Durable object storage, access control, encryption, backups, lifecycle policies, scalable query options | Schema design, data loading, retention rules, cost governance | Low to medium | Medium to high |
| Specialized data platform SaaS | Managed warehouse/lakehouse platforms | Analytics-oriented storage, governance features, scalable querying, integrations | Data modeling, ingestion jobs, access policies | Low to medium | Medium to high |
| Build from scratch | Custom file and metadata storage | Theoretically full control | Durability, backups, access control, indexing, query layer, disaster recovery | Very high | Low, but not practical |

**Recommendation:** Use managed object storage for raw files and managed structured storage for metadata and annotations. Storage durability, backups, and access control are not good candidates for custom implementation.

## 5. Identity and Access Control

The system needs centralized access control for uploaders, reviewers, analysts, administrators, and service-to-service communication between pipeline components.

| Option | Examples | What You Get Out of the Box | What Still Needs to Be Built | Operational Burden | Lock-in Risk |
|---|---|---|---|---|---|
| Open-source identity provider | Keycloak | Centralized authentication, groups, roles, SSO support, self-hosting | Deployment, upgrades, backups, high availability, integration with each service | Medium to high | Low |
| Cloud provider identity and IAM | Google Cloud IAM / Identity Platform, Azure Entra ID, cloud IAM services | Managed users or federated identities, service accounts, role-based permissions, audit logs, integration with cloud services | Role design, application integration, least-privilege policies | Low to medium | Medium to high |
| Specialized identity SaaS | Okta, Auth0 | Mature SSO, user lifecycle management, MFA, integrations | Application integration, role mapping, vendor governance | Low | Medium to high |
| Build from scratch | Custom auth and permissions | Exact control over auth behavior | Authentication, authorization, password/security flows, audit logs, compliance, maintenance | Very high | Low, but not practical |

**Recommendation:** Use the identity and IAM capabilities of the selected primary cloud platform, or integrate an established identity provider with that platform. Building custom authentication is not justified. Access should be role-based, with separate roles for uploaders, reviewers, analysts, administrators, and pipeline service accounts.

## Overall Direction

A practical design for this project is a cloud-centered hybrid approach:

- Select one primary cloud platform for identity, raw storage, queueing, processing, OCR, structured storage, and monitoring.
- Because Ukrainian OCR support is required, prefer a platform whose managed OCR supports Ukrainian, such as Google Cloud or Azure, rather than an AWS Textract-centered design.
- Use managed cloud services where the platform provides hard-to-build capabilities, especially raw file durability, access control, scalable processing, and OCR/document understanding.
- Use open-source or simpler managed tools where the workflow is less specialized, especially annotation at the initial scale, but host them inside the selected cloud environment.
- Avoid building custom infrastructure for OCR, storage, and annotation unless there is a strong business requirement that existing tools cannot satisfy.

For example, a coherent final implementation could use a Google Cloud-centered stack: Cloud IAM / Identity Platform for access control, Cloud Storage for raw images, Pub/Sub for processing jobs, Cloud Run workers for pre-processing and metadata extraction, Cloud Vision OCR for Ukrainian-capable image OCR, BigQuery for analysis-ready structured data, Cloud SQL for Label Studio operational state, Label Studio hosted in the same environment for annotation, and Cloud Monitoring for observability.

This keeps the solution realistic for the expected scale while still leaving room to scale from approximately `30,000` to `300,000` materials per month.
