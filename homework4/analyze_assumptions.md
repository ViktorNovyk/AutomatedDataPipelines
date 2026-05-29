# Analyze Assumptions

The system collects direct marketing materials from different parts of the country to understand what types of creatives are used across different industries.

The initial scope is limited to common direct marketing materials uploaded as image files from scans or photos.

## Core Creative Formats

- `flyer`
- `brochure`
- `postcard / mailer`
- `coupon / voucher`
- `catalog / product sheet`
- `other`

## Core Creative Intents

- `discount / promotion`
- `new product or service`
- `event invitation`
- `brand awareness`
- `informational`
- `other`

## Core Industry Labels

- `retail`
- `food and restaurants`
- `healthcare`
- `finance`
- `real estate`
- `education`
- `automotive`
- `beauty / wellness`
- `other`

PDF documents and digital-only channels such as social media ads, banners, SMS campaigns, and landing pages are out of scope for the initial design.

OCR is required because most materials contain useful text that should be searchable and available for metadata extraction.

Ukrainian language support is required for OCR because materials collected around the country may contain Ukrainian text. Any OCR service or library considered for the solution must support Ukrainian text recognition.

Human annotation is required because creative format, intent, and industry can be ambiguous. OCR and metadata extraction support reviewers, but the final labels are assigned manually.

The system is not real-time. Processing may happen in batch or near-batch mode, and materials should become available for review and analysis within one business day.

Raw files should be preserved unchanged so they can be audited, reprocessed, or re-annotated later if OCR logic, classification rules, or taxonomy changes.

## Scale, Volume, and Latency Assumptions

- Initial scale: approximately `30,000` materials per month.
- Peak ingestion: up to `3,000` materials in a daily batch.
- Target scale: up to `300,000` materials per month without a full redesign.
- Average file size: approximately `2 MB` per material.
- Supported document shape: single-page image documents.
- Supported input formats: `JPG`, `JPEG`, and `PNG`.
- Processing latency: OCR and metadata extraction should normally complete within a few hours.
- Business SLA: uploaded materials should be available for annotation within one business day.
