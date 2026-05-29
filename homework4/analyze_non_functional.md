# Non-Functional Requirements

The system is designed for batch or near-batch processing of single-page direct marketing materials. It does not require real-time processing, but it should be reliable, scalable, and cost-conscious.

## 1. Throughput

- The system should handle approximately `30,000` uploaded materials per month at the initial scale.
- The system should support upload peaks of up to `3,000` files in a daily batch.
- OCR and metadata extraction should be scalable independently from upload and storage.

## 2. Latency

- Real-time processing is not required.
- Uploaded materials should become available for annotation within the same business day.
- Under normal load, OCR and metadata extraction should complete within a few hours.

## 3. Availability

- Raw file storage should be highly available because it is the source of truth.
- Ingestion should tolerate temporary processing failures without losing uploaded files.
- Annotation and analysis interfaces can have lower availability requirements than raw storage.

## 4. Scalability

- The architecture should scale from approximately `30,000` to `300,000` materials per month without a full redesign.
- Compute-heavy stages, especially OCR and pre-processing, should be horizontally scalable.
- Storage should support growing volumes of raw files, OCR text, metadata, and annotations.

## 5. Reliability

- The system should not lose uploaded files after successful ingestion.
- Failed OCR or metadata extraction jobs should be marked as failed and available for retry.
- Processing status should be tracked for each material.

## 6. Security and Compliance

- Uploaded materials should be accessible only to authorized users.
- Raw files and structured metadata should not be publicly accessible.
- Different user roles may exist, for example uploader, reviewer, and analyst.
- Access should be managed through a central identity provider or cloud IAM service.
- Some materials may contain contact information, addresses, names, or phone numbers, so extracted data should be stored and exposed carefully.
- Retention rules should be defined for raw files and extracted metadata.

## 7. Cost Efficiency

- The design should avoid always-on compute where batch or event-driven processing is enough.
- OCR can become a major cost driver, so the system should avoid unnecessary reprocessing.
- Storage cost should be considered separately for raw files and structured data.

## 8. Observability

- The system should expose processing status and basic operational metrics.
- Important metrics include number of uploaded files, OCR success/failure rate, annotation backlog, and processing time.
- Errors should be logged in a way that supports investigation and retry.

## 9. Data Quality

- OCR quality should be tracked where possible using confidence scores or failure flags.
- OCR quality should be validated on Ukrainian-language materials because Ukrainian support is a mandatory assumption.
- Materials with low-quality OCR should be marked for manual review.
- Annotation labels should use a controlled taxonomy to keep analysis consistent.
