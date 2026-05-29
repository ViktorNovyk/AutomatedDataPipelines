# OCR Service Pricing and Recommendation

This section analyzes OCR service options because OCR is central to the pipeline, usage-based, and affected by the Ukrainian language requirement.

The estimate uses the project assumptions:

- Initial scale: `30,000` single-page images per month.
- Target scale: `300,000` single-page images per month.
- Input formats: `JPG`, `JPEG`, `PNG`.
- Ukrainian OCR support is required.
- General image OCR is sufficient for the first version; document AI can be considered later if layout-aware extraction becomes required.

## Pricing and Feature Comparison

| Option                          | Pricing Model                                                                                                                                                                                                                                                   |                             Estimated Monthly Cost at 30,000 Images |  Estimated Monthly Cost at 300,000 Images | Ukrainian Support                         | Features                                                                                                            | Advantages                                                                                                                         | Disadvantages                                                                                                     | Integration Fit                                                     |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------:|------------------------------------------:|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| Google Cloud Vision OCR         | Pay per image/unit. First `1,000` units/month are free. Text Detection / Document Text Detection is `$1.50` per `1,000` units up to 5M units/month. Source: [Google Cloud Vision pricing](https://cloud.google.com/vision/pricing)                              |                                                     Around `$43.50` |                          Around `$448.50` | Yes                                       | General image OCR, text detection, text locations, confidence-related output, API integration                       | Good fit for image-only input, simple pricing, strong integration with Google Cloud Storage, Pub/Sub, Cloud Run, BigQuery, and IAM | Usage cost grows with volume; cloud lock-in; OCR quality should still be tested on Ukrainian marketing images     | Strong if the final stack is Google Cloud-centered                  |
| Azure AI Vision OCR / Read      | Pay per transaction/page depending on selected Azure OCR capability and region. First 1M transactions `$1.00` per `1,000` transactions. Source: [Azure Read API docs](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/how-to/call-read-api) |                                                      Around `$30.0` |                           Around `$300.0` | Yes                                       | Image OCR, Read OCR, text extraction from images; Azure also separates general image OCR from Document Intelligence | Good alternative if the final stack is Azure-centered; integrates with Azure storage, Entra ID, and Azure monitoring               | Integration is less smooth if the rest of the stack is Google Cloud                                               | Strong if the final stack is Azure-centered                         |
| ABBYY Vantage                   | Commercial SaaS subscription / page-count model. Exact price depends on contract and package.                                                                                                                                                                   |                                               Requires vendor quote |                     Requires vendor quote | Yes                                       | Strong OCR and intelligent document processing, broad language support, document workflows                          | Strong OCR/language capabilities; useful when OCR quality is the top priority                                                      | Less seamless with cloud-native pipeline services; likely higher cost; additional vendor relationship and lock-in | Medium; good OCR component, but external to the main cloud platform |
| Tesseract / EasyOCR / PaddleOCR | No per-image API fee; infrastructure cost depends on worker compute, storage, and operations                                                                                                                                                                    | Low direct software cost; compute and ops cost depend on deployment | Scales with worker compute and operations | Possible with language/model setup        | Local OCR processing, configurable models, full control over data                                                   | Low vendor lock-in; useful for experiments or cost-sensitive workloads                                                             | More tuning, scaling, quality testing, monitoring, and maintenance; accuracy may be weaker on noisy scans/photos  | Medium; can run in any cloud, but team owns operations              |
| AWS Textract                    | Pay per page/API operation, but not recommended for this project                                                                                                                                                                                                |                                                       Not estimated |                             Not estimated | No Ukrainian support for this requirement | Managed document OCR/extraction                                                                                     | Good AWS-native integration when supported languages are enough                                                                    | Does not satisfy Ukrainian OCR requirement, so it conflicts with assumptions                                      | Poor fit for this project                                           |

## Cost Calculation for Google Cloud Vision OCR

Google Cloud Vision pricing lists the first `1,000` units per month as free and `Text Detection` / `Document Text Detection` at `$1.50` per `1,000` units for usage from `1,001` to `5,000,000` units per month.

Initial scale:

```text
(30,000 - 1,000) / 1,000 * $1.50 = $43.50/month
```

Target scale:

```text
(300,000 - 1,000) / 1,000 * $1.50 = $448.50/month
```

These estimates cover OCR API calls only. They do not include raw image storage, processing workers, monitoring, retries, reprocessing, annotation tooling, or structured storage.

## Recommendation

Use Google Cloud Vision OCR as the first-choice OCR service for the current design.

It fits the current scope because the input is image-only, Ukrainian OCR support is required, and the project needs searchable text plus simple metadata extraction rather than complex layout-aware document understanding. It also integrates smoothly with a Google Cloud-centered architecture using Cloud Storage, Pub/Sub, Cloud Run, BigQuery, Cloud SQL for annotation tool state, Cloud IAM / Identity Platform, and Cloud Monitoring.

Azure AI Vision OCR is a reasonable alternative if the final architecture is Azure-centered. ABBYY Vantage is a strong specialized OCR option if OCR quality and language coverage are more important than cloud-native integration. Open-source OCR is useful for experimentation or cost-sensitive deployments, but it shifts quality tuning, scaling, and operations to the implementation team.

AWS Textract is not recommended because it does not satisfy the Ukrainian OCR requirement.
