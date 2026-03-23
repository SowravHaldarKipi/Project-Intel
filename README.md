
# Cortex Project Intel – AI‑Powered Project Planning with Snowflake Cortex

[![Streamlit App](https://img.shields.io/badge/Streamlit-App-brightgreen)](https://streamlit.io)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cortex-blue)](https://www.snowflake.com/cortex/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Cortex Project Intel** is a prototype application that uses Snowflake Cortex to generate comprehensive project plans from a Statement of Work (SOW) PDF, enriched with patterns from your institutional memory of past completed projects.

---

## ✨ Features

- 📄 **SOW PDF ingestion** – Upload a Statement of Work; Cortex extracts deliverables, acceptance criteria, constraints, and out‑of‑scope items.
- 🧠 **Institutional memory** – Learns from your database of completed projects to reuse risks, test patterns, and timeline lessons.
- 📋 **Full plan generation** – Produces a complete project plan with:
  - Executive summary
  - RAID log (Risks, Assumptions, Issues, Dependencies)
  - Test cases
  - Delivery timeline
  - Deployment strategy
  - SOW traceability matrix
- 🏃 **Visual WBS / sprint board** – Structured work breakdown with sprints or phases.
- 💬 **Q&A on the plan** – Ask Cortex anything about the plan, risks, tasks, emails, RACI.
- ⚠️ **Risk analyser** – Extract and score risks from the plan.
- 📤 **Export** – Save plan to Snowflake `projects` table, download as text, or generate a professional 5‑sheet Excel workbook.

---

## 🏗️ Architecture Overview


<img width="2016" height="1370" alt="_- visual selection (38)" src="https://github.com/user-attachments/assets/e04a411a-3119-434c-9bd1-81d95d598fa3" />


- **Streamlit** – interactive dashboard.
- **Snowpark** – executes SQL, calls Cortex, reads/writes `projects` table.
- **Cortex** – completes prompts for SOW analysis, pattern extraction, WBS generation, plan assembly, and Q&A.
- **Openpyxl** – builds Excel workbook with 5 sheets (optional, used in export).

---

## 🧰 Prerequisites

- Snowflake account with `ACCOUNTADMIN` privileges.
- A warehouse (e.g., `COMPUTE_WH`).
- Snowflake Notebook environment (or local Python with `snowflake-snowpark-python` and `streamlit` installed).
- Cortex enabled in your region (supports `mixtral-8x7b`).
- Python packages: `PyPDF2` (for PDF text extraction), `openpyxl` (for Excel export).

---

## 🚀 Setup Instructions

1. **Clone the repository**  
```
   git clone https://github.com/your-org/cortex-project-intel.git
   cd cortex-project-intel
Run the Snowflake setup script
Execute setup.sql in Snowsight or a SQL client. It creates:

Database COCO_HACKATHON_DB, schema CORE_DATA.

Table projects (with sample completed projects for institutional memory).

The cortex_complete function (if it doesn’t already exist).

Install Python dependencies

bash
pip install streamlit snowflake-snowpark-python PyPDF2 openpyxl plotly
Run the Streamlit app
Inside a Snowflake Notebook, paste the code from app.py into a cell and run. For local testing, modify the session creation.
```

## 📖 How to Use
1. Upload a Statement of Work (SOW) PDF – Required. Cortex will extract key information.
2. Fill project details – Name, description, technologies, customer, type, team size, duration, methodology, etc.
3. Optionally upload CSV project plans – Multiple CSV files (with columns like Task, Subtask, Start Date, End Date) become part of the institutional memory.
4. Click “Generate Project Plan” – Cortex will:
- Analyse the SOW.
- Match past projects (from the database and uploaded CSVs) to extract reusable patterns.
- Build a structured WBS as sprints/phases.
- Assemble the final plan with all sections.
- Explore results – Use the tabs:
- Full Plan – The complete generated plan with formatted tables.
- WBS / Sprints – Visual sprint or phase breakdown.
- SOW Analysis – What Cortex extracted from the PDF.
- Ask the Plan – Q&A on any aspect.
- Risk Analyser – Extract and score risks.
- Export – Save to Snowflake, download text, or generate Excel workbook.

## 🧪 Demo Walkthrough (with sample data)
After running setup.sql, you’ll have a few completed projects in the projects table (e.g., “Legacy Data Migration 2022”, “Customer 360 Analytics Platform”). Upload a sample SOW PDF (you can create a simple one‑page document) and generate a plan. The app will:

Extract deliverables from the SOW.

Select the most similar past projects.

Adapt their risks, test patterns, and timeline lessons.

Produce a full plan with RAID log, test cases, and sprint board.

## 🛠️ Technology Stack

<img width="1863" height="850" alt="_- visual selection (39)" src="https://github.com/user-attachments/assets/6e8e22b4-0053-42b4-8b67-aec159add6ce" />


📁 Project Structure
```
cortex-project-intel/
├── app.py                # Streamlit application
├── setup.sql             # Snowflake database setup (projects table, cortex function)
├── README.md             # This file
├── Project_Summary.md    # One‑page executive summary
└── LICENSE               # MIT License
```



