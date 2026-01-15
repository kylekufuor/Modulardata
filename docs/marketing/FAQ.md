# Frequently Asked Questions

> **Status:** Placeholder
> **Last Updated:** 2026-01-15

---

## General

### What is ModularData?
ModularData is an AI-powered API that transforms CSV data using natural language instructions. Instead of writing code, you describe what you want in plain English.

### Who is it for?
Data analysts, business users, and developers who need to clean and transform data without writing pandas or SQL code.

### How does the AI work?
ModularData uses GPT-4 to understand your intent and generate the appropriate data transformations. The AI has been trained to understand common data cleaning tasks.

---

## Technical

### What file formats are supported?
Currently CSV files only. Excel and JSON support is planned.

### How large of a file can I upload?
Up to 50MB. Larger file support is planned.

### Is my data secure?
Data is encrypted in transit (HTTPS) and at rest (Supabase). Data is sent to OpenAI for processing. See our Privacy Policy for details.

### Can I use this programmatically?
Yes, ModularData is API-first. All functionality is available via REST API.

---

## Usage

### What transformations are available?
53 transformations including: remove rows, fill nulls, deduplicate, change case, parse dates, and more. See the [Transformation Catalog](../users/TRANSFORMATION_CATALOG.md).

### Can I undo changes?
Yes. Every transformation creates a version. You can rollback to any previous version.

### What if the AI doesn't understand me?
Try being more specific. Reference exact column names. If issues persist, contact support.

---

## Pricing

### Is ModularData free?
Currently in free beta. Pricing will be introduced for production use.

### Will there be a free tier?
Pricing is not finalized, but we plan to offer a free tier for small-scale use.

---

## Future Work

- [ ] Expand FAQ based on user questions
- [ ] Add troubleshooting section
- [ ] Create searchable FAQ system

---

*Questions? Contact us at [email placeholder].*
