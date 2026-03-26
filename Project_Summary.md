# Cortex Project Intel – AI-Powered Project Planning from Institutional Memory

## Project Summary

**Cortex Project Intel** is a Streamlit application that leverages Snowflake Cortex to transform unstructured Statements of Work (SOWs) and historical project data into comprehensive, AI‑generated project plans. It automatically builds a Work Breakdown Structure (WBS), RAID log, test cases, delivery timeline, and ROI analysis – all tailored to your organisation’s past project patterns.

---

## 🎯 Purpose

Project managers and PMOs spend countless hours manually planning projects, extracting deliverables from SOWs, and reinventing the wheel for each new initiative. Cortex Project Intel solves this by:

- **Automating plan generation** – Upload a SOW PDF, and Cortex extracts deliverables, creates a detailed WBS (sprints/phases), and populates RAID logs and test cases.
- **Leveraging institutional memory** – Past projects stored in Snowflake (or uploaded CSVs) are used as templates to recommend risks, test patterns, and timeline lessons.
- **Delivering immediate ROI** – Quantified savings in planning time, risk avoidance, and faster time‑to‑value.
- **Empowering PMOs** – A self‑service interface where any project manager can generate a complete project plan in minutes.

---

## 🔑 Key Features

| Feature | Description |
|---------|-------------|
| **📄 SOW Upload** | Upload a PDF Statement of Work. Cortex extracts objectives, deliverables, requirements, constraints, and risks. |
| **📂 CSV Import** | Upload multiple CSV project plans to build institutional memory. The app parses tasks, dates, and descriptions into structured WBS. |
| **🏛️ Institutional Memory** | Past projects are stored in Snowflake (or imported CSVs). Cortex selects the most relevant past projects to use as templates for RAID logs, test cases, and timeline patterns. |
| **🚀 AI Plan Generation** | One‑click generation of a complete project plan with seven standard sections: Executive Summary, RAID Log, Test Cases, Delivery Timeline, Deployment Strategy, SOW Traceability Matrix, Lessons from Past Projects. |
| **🏃 WBS / Sprint Board** | Detailed work breakdown structured as sprints (Agile) or phases (Waterfall). Each sprint includes name, weeks, goal, tasks, and deliverable – all derived directly from the SOW. |
| **⚠️ Risk Analyser** | Extract and score risks from the generated plan. Then query past projects to get lessons learned and guardrails for those risk types. |
| **💬 Ask the Plan** | Natural language Q&A on the generated plan. Ask “What is the critical path?” or “Write a kick‑off email” – Cortex answers using the plan context. |
| **💰 ROI Dashboard** | Real‑time ROI calculation based on planning time saved, risk avoidance, and project duration. Shows cost‑benefit breakdown, payback period, and value visualisations. |
| **📤 Export** | Save the project plan to Snowflake for future reuse, download as text, or generate a comprehensive Excel workbook with 5 sheets: Summary, WBS/Sprints, RAID Log, Test Cases, Timeline. |

---

## 🧱 Technology Stack

- **Frontend** – Streamlit (custom CSS for a clean, modern PMO interface)
- **Backend / Data** – Snowflake Snowpark for metadata queries and session management
- **AI Engine** – Snowflake Cortex (`cortex_complete`) for natural language processing:
  - SOW analysis
  - WBS extraction
  - RAID log generation
  - Test case creation
  - Lessons learned synthesis
- **Document Parsing** – PyPDF2 for PDF extraction
- **Spreadsheet Generation** – openpyxl for Excel workbook creation
- **Data Manipulation** – Pandas for dataframes
- **Hosting** – Snowflake‑hosted Streamlit (or any environment with Snowpark connectivity)

---

## ⚙️ How It Works

1. **Upload SOW & Fill Project Details**  
   - User uploads a PDF SOW. Cortex extracts all relevant sections (objectives, deliverables, requirements, constraints).
   - User provides project name, description, technologies, team size, methodology (Agile/Waterfall/Hybrid/SAFe), duration, risk appetite, compliance needs, etc.

2. **Load Institutional Memory**  
   - The app queries Snowflake for past projects (or uses uploaded CSVs).  
   - Cortex selects the 4 most similar past projects based on description and technologies. Their RAID logs, test cases, and timeline lessons are retrieved.

3. **Generate Work Breakdown Structure**  
   - Using the SOW analysis, Cortex creates a structured WBS with sprints/phases.  
   - Each sprint contains real tasks, goals, and deliverables derived directly from the SOW – no generic placeholders.  
   - A fallback parsing mechanism ensures WBS generation even if the AI response is incomplete.

4. **Assemble Final Plan**  
   - A final Cortex prompt combines the SOW analysis, WBS, and past project patterns to produce a complete plan with 7 sections.  
   - Sections are formatted with Markdown tables for easy rendering.

5. **Display & Interact**  
   - The plan is displayed in a tabbed interface:
     - **Full Plan** – Rendered with styled tables, lists, and headers.
     - **WBS / Sprints** – Visual sprint board with detailed tasks.
     - **SOW Analysis** – Extracted SOW sections with icons.
     - **Ask the Plan** – Q&A with context from the generated plan.
     - **Risk Analyser** – Extract risks, then query past projects for lessons.
     - **ROI Report** – Detailed ROI breakdown and visualisations.
     - **Export** – Save to Snowflake, download text, or generate Excel.

6. **Export & Save**  
   - Save the plan to the `projects` table in Snowflake for future reuse.  
   - Download as `.txt` or `.xlsx` (Excel with 5 sheets).

---

## 💡 Value Proposition

| Stakeholder | Benefit |
|-------------|---------|
| **PMO / Project Manager** | Reduce planning time from days to minutes. Leverage past project knowledge without manual research. |
| **Executive Sponsor** | Clear, AI‑generated plans with quantified ROI and risk analysis. Faster time‑to‑value. |
| **Technical Teams** | Detailed task breakdowns derived directly from SOW, reducing misinterpretation. |
| **Business Analysts** | Automated extraction of requirements, traceability, and test cases from SOW. |

**Quantified Impact** (example based on a 12‑week, 5‑person project):  
- Planning time reduction: **67%** (12 hours → 4 hours)  
- Risk avoidance: **£30,000** (6 risks identified and mitigated)  
- Total ROI: **225%**  
- Payback period: **3 days**

---

## 🖥️ User Interface Overview

- **Sidebar** – All inputs: SOW upload, CSV upload, project details, methodology, team roles, compliance, ROI inputs, health score, effort estimator, and smart AI tips.
- **ROI Dashboard** – Always visible at the top, showing real‑time ROI metrics and composition.
- **Institutional Memory Panel** – Lists past projects from database and imported CSVs; click to preview their content.
- **Main Area** – After generation, a set of tabs for each part of the plan.
- **Export Section** – Save to Snowflake, download text or Excel.

---

## 🚀 Future Enhancements

- **Natural Language SOW Upload** – Allow users to paste SOW text instead of PDF.
- **Gantt Chart Visualisation** – Display timeline as an interactive Gantt chart.
- **Resource Allocation** – Automatically assign tasks to roles based on team roster.
- **Integration with Jira / Azure DevOps** – Export sprints directly to project management tools.
- **Real‑time Plan Editing** – Allow users to edit the plan and re‑generate dependent sections (e.g., RAID log after updating risks).

---

## 📄 Conclusion

Cortex Project Intel demonstrates how Snowflake Cortex can be used to automate one of the most time‑consuming tasks in project management – planning. By combining AI with institutional memory, it delivers high‑quality, context‑aware project plans in minutes, freeing PMOs to focus on execution rather than administration. The built‑in ROI tracking and risk analysis provide clear business justification for adopting AI‑assisted planning.
