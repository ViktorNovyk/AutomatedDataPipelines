# Functional Requirements

The system should support a simple pipeline for collecting, processing, annotating, and storing direct marketing materials for later analysis.

## 1. Ingestion

- Accept single-page marketing materials as JPG, JPEG, or PNG image files.
- Support manual upload and batch upload.
- Validate file type and reject unsupported or corrupted files.
- Assign a unique ID to each uploaded material.
- Capture basic technical metadata automatically:
  - original file name
  - file type
  - file size
  - upload timestamp

## 2. Raw Storage

- Store the original uploaded file unchanged.
- Keep a reference between the raw file and all extracted data.

## 3. Pre-processing

- Normalize the document image for OCR where needed, for example orientation, resolution, or contrast.

## 4. OCR

- Extract text from each uploaded material.
- Store extracted text with the material record.
- Mark files where OCR fails or produces low-quality results.
- OCR extracts text only. It does not assign the final creative labels.

## 5. Metadata Extraction

- Extract simple metadata from OCR text where possible:
  - language
  - phone number or website
  - discount or price information
- Optionally extract brand or company name when it can be detected with reasonable confidence.
- Store extracted metadata with the material record.

## 6. Annotation

- Provide or integrate an annotation interface where reviewers can view the original material and OCR text.
- Use OCR text as supporting context for reviewers.
- Allow human reviewers to assign the core labels:
  - `creative_format`
  - `creative_intent`
  - `industry`
- Track annotation status:
  - `pending`
  - `annotated`
  - `rejected`

## 7. Structured Storage and Analysis Access

- Store OCR text, extracted metadata, labels, and processing status in structured storage.
- Keep operational annotation state separate from the final analysis-ready dataset if the annotation tool requires its own database.
- Allow the resulting dataset to be queried or exported for analysis.
