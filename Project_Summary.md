# Cortex Project Intel – Project Summary

## One‑Page Executive Overview

**Cortex Project Intel** is a prototype application that transforms a Statement of Work (SOW) into a complete, actionable project plan by combining **Snowflake Cortex AI** with **institutional memory** from past completed projects. It reduces planning time from weeks to minutes and ensures new projects benefit from lessons learned.

---

### 🎯 Business Problem

Project planning is slow, manual, and often repeats the same mistakes. New projects start from scratch, ignoring risks and test patterns from past successes and failures. **Cortex Project Intel** solves this by:

- **Automatically extracting** deliverables, acceptance criteria, and constraints from any SOW PDF.
- **Matching** the new project to the most relevant past projects (from your Snowflake `projects` table or uploaded CSVs).
- **Reusing** risks, test patterns, and timeline lessons from those past projects.
- **Generating** a complete plan with RAID log, test cases, sprint board, and deployment strategy.

---

### 🧠 How It Works

1. **SOW ingestion** – User uploads a PDF. Cortex extracts:
   - Project objective
   - Key deliverables (numbered list)
   - Acceptance criteria per deliverable
   - Out of scope items
   - Constraints (budget, timeline, compliance)

2. **Institutional memory scan** – Cortex reads your `projects` table (or uploaded CSV plans) and selects up to 4 projects most similar to the new one. It extracts:
   - Risks and their mitigations
   - Test patterns (types, scenarios, expected results)
   - Timeline lessons (e.g., “integration testing took 30% longer”)

3. **WBS generation** – Using the SOW deliverables, Cortex builds a work breakdown structure as sprints (Agile) or phases (Waterfall), mapping each task directly to a SOW deliverable.

4. **Plan assembly** – A final Cortex call assembles the full plan, including:
   - Executive summary
   - RAID log (table)
   - Test cases (table)
   - Delivery timeline (table)
   - Deployment strategy
   - SOW traceability matrix
   - Lessons from past projects

5. **User interaction** – The user can ask questions about the plan, extract risks, and export the plan to Snowflake, as text, or as an Excel workbook.

---

### 🔧 Technical Architecture

| Layer                | Technology                               |
|----------------------|------------------------------------------|
| **UI**               | Streamlit (custom dark theme)            |
| **Backend**          | Snowpark Python – SQL and Cortex calls   |
| **LLM**              | `mixtral-8x7b` via `cortex_complete`     |
| **PDF extraction**   | PyPDF2                                   |
| **Storage**          | Snowflake (`projects` table)             |
| **Excel export**     | Openpyxl                                 |

---

### 📊 Key Metrics (with Sample Data)

| Metric                     | Value                     |
|----------------------------|---------------------------|
| Time to generate a plan    | ~30 seconds (4 Cortex calls) |
| SOW analysis accuracy      | High for structured PDFs  |
| Past projects matched      | Up to 4                   |
| Plan sections              | 8 (exec summary, RAID, tests, timeline, etc.) |
| Excel sheets               | 5 (Summary, WBS, RAID, Tests, Timeline) |

---

### ✅ Strengths (Prototype)

- **Zero manual WBS** – Derived entirely from the SOW.
- **Pattern reuse** – Risks and test cases are not generic; they are adapted from real past projects.
- **Full traceability** – SOW traceability matrix links deliverables to sprints.
- **Export ready** – Excel workbook is professional, with formatting and color coding.

---

### 🚧 Limitations (to address for production)

- **PDF parsing** – Relies on PyPDF2; may not extract text from scanned PDFs.
- **Token limits** – Long SOWs or large `projects` tables may need chunking.
- **SQL injection** – Use parameterised queries.
- **Error handling** – Add retries for Cortex calls.

---

### 🗺️ Next Steps (if moving to production)

1. **Improve PDF extraction** – Use OCR for scanned documents.
2. **Parameterise all SQL** queries.
3. **Add user authentication** and role‑based access to `projects`.
4. **Implement incremental learning** – automatically use newly completed projects for future plans.
5. **Enhance WBS generation** – allow manual editing of sprint tasks.

---

### 📦 Deliverables

- Streamlit app (`app.py`).
- Snowflake setup script (`setup.sql`).
- README and this summary.
- Sample `projects` data for demonstration.

---

**Cortex Project Intel demonstrates how Snowflake Cortex can turn a simple SOW into a full, professionally formatted project plan, leveraging the collective intelligence of your organisation to avoid repeated mistakes and accelerate delivery.**
