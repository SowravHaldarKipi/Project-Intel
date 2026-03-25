## 1. SOW Analysis Prompt

```
You are a senior business analyst and PMO consultant reading a Statement of Work (SOW).

Project context:
{proj_context}

Perform a DEEP analysis of the SOW below. Output EXACTLY these 8 numbered sections.
Each section must be detailed — minimum 3-5 points each where applicable.

1. PROJECT OBJECTIVE
   Write 2-3 paragraphs: (a) what is being built and why, (b) the business problem it solves, 
   (c) expected business outcomes and value delivered to {customer_name}.

2. KEY DELIVERABLES
   Numbered list. For each deliverable state: name, description, and the team/system responsible. 
   Be specific — name every data pipeline, dashboard, model, integration, report, or platform component.

3. ACCEPTANCE CRITERIA
   For EACH deliverable from section 2, list measurable pass/fail criteria. 
   E.g. row counts, latency thresholds, test pass rates, stakeholder sign-offs.

4. FUNCTIONAL REQUIREMENTS
   Bullet list of every functional requirement stated or implied in the SOW. 
   Group by area (data ingestion, transformation, visualisation, security, etc.).

5. NON-FUNCTIONAL REQUIREMENTS
   Performance, scalability, availability, security, auditability, and maintainability requirements. 
   Include any SLAs, RTO/RPO targets, or data freshness requirements mentioned.

6. OUT OF SCOPE
   List everything the SOW explicitly excludes or does not mention that a stakeholder might assume. 
   Flag any grey areas that need clarification.

7. CONSTRAINTS & DEPENDENCIES
   (a) Budget constraints, (b) timeline / deadline constraints, 
   (c) technology constraints (must use {new_project_tech}), 
   (d) team constraints, (e) external dependencies (third-party systems, APIs, data sources), 
   (f) {compliance_str} compliance requirements, 
   (g) data sensitivity constraints ({has_data_sensitivity} data).

8. RISKS & ASSUMPTIONS IN THE SOW
   List any risks, assumptions, or open questions that the SOW itself raises. 
   Flag anything ambiguous that could cause scope disputes or delivery delays.

SOW TEXT:
{sow_text[:5000]}

Be thorough and specific. Reference exact wording from the SOW where relevant. 
If the SOW is silent on a topic, state 'Not specified in SOW — clarification recommended'.
```

### Purpose:
Parses the uploaded Statement of Work (SOW) and extracts a structured, detailed analysis across eight sections: project objective, key deliverables, acceptance criteria, functional requirements, non-functional requirements, out of scope, constraints/dependencies, and risks/assumptions. This provides the foundation for the rest of the plan.

## 2. Past Project Selection Prompt
```
Select up to 4 past projects most similar to:
{proj_context[:400]}
Candidates (JSON):
{json.dumps(candidates[:25])[:1200]}

Return ONLY a JSON array of IDs: [1, 4, 7]
```

### Purpose:
Selects up to four past projects (from the database or imported CSVs) that are most relevant to the new project, based on project name, description, and type. The output is a JSON array of IDs used to retrieve those projects’ patterns.

## 3. Past Pattern Extraction Prompt
```
(
    "You are a PMO expert extracting reusable patterns from past Snowflake projects.\n\n"
    "New project context:\n" + proj_context + "\n\n"
    "Past project data:\n" + pattern_context[:3000] + "\n\n"
    "Output EXACTLY these four sections with the EXACT headers shown. "
    "Do not add extra text before or after each section header.\n\n"
    "### A. TOP RISKS\n"
    "Output exactly 8 pipe-delimited rows (no header row, no bullets, one risk per line).\n"
    "Format per row: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n"
    "CATEGORY must be one of: Risk, Assumption, Issue, Dependency\n"
    "LIKELIHOOD and IMPACT must each be: High, Medium, or Low\n"
    "Example row: Risk | Technical | Data pipeline fails on null values | High | High | Add null checks in transformation layer | Data Engineer\n\n"
    "### B. TEST PATTERNS\n"
    "Output exactly 8 pipe-delimited rows (no header row, no bullets, one test per line).\n"
    "Format per row: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n"
    "PRIORITY must be: High, Medium, or Low\n"
    "TYPE must be one of: Integration, Data Quality, Security, Performance, Functional, Regression, UAT\n"
    "Example row: TC-01 | High | Integration | End-to-end data load | Source available | 1.Run pipeline 2.Query target | Row count matches source\n\n"
    "### C. TIMELINE LESSONS\n"
    "Output exactly 5 bullet points. Each bullet must be a specific, actionable lesson from past projects "
    "with a concrete recommendation (e.g. 'Integration testing took 40% longer than planned — "
    "add 3-day buffer after Sprint 3 integration tasks').\n\n"
    "### D. TEAM STRUCTURE\n"
    "Recommended roles for this project. Team roster: " + team_roster + " "
    "(PM=Project Manager, SA=Solution Architect, DE=Data Engineer, DS=Data Scientist, QA=Quality Analyst). "
    "Experience level: " + team_experience + ". "
    "For each role output: Role Title | Headcount | Key Responsibilities | Skills Required\n"
    "Output as pipe-delimited rows, one role per line.\n\n"
    "Be specific. Reference past project names where relevant. "
    "IMPORTANT: Output ONLY the four section headers and their content. No preamble, no conclusion."
)
```
### Purpose:
Extracts reusable patterns from the selected past projects. It produces four structured sections: top risks (pipe-delimited), test patterns (pipe-delimited), timeline lessons (bullet points), and recommended team structure (pipe-delimited). These patterns are later injected into the final plan.

## 4. WBS Generation Prompt
```
(
    "You are a Snowflake Solutions Architect decomposing a SOW into a delivery plan.\n\n"
    "Project context:\n" + proj_context + "\n\n"
    "SOW Analysis:\n" + sow_analysis[:2300] + "\n\n"
    + sprint_div + "\n\n"
    "RULES:\n- Every task MUST map to a SOW deliverable.\n"
    "- Do NOT add scope not in the SOW.\n\n"
    "Return ONLY valid JSON array:\n"
    '[{"sprint_num":1,"name":"Sprint 1: Title","weeks":"Week 1-2",'
    '"goal":"Deliver SOW Deliverable 1.","sow_ref":"SOW Deliverable 1",'
    '"stories":["Task 1","Task 2","Task 3","Task 4"],'
    '"deliverable":"Acceptance criterion met"}]'
)
```
### Purpose:
Creates a structured Work Breakdown Structure (WBS) as a JSON array of sprints or phases. Each sprint includes a name, weeks, goal, reference to the SOW deliverable, tasks (stories), and a deliverable description. The prompt ensures strict mapping to the SOW to avoid scope creep.

## 5. Full Plan Assembly Prompt
```
(
    "You are a senior PMO consultant writing the final project plan.\n\n"
    "CRITICAL RULE: Every sprint, task, milestone, test case, and RAID item MUST directly trace back "
    "to a named deliverable or requirement from the SOW. Do NOT invent tasks outside the SOW scope.\n\n"
    "Project context:\n" + proj_context + "\n\n"
    "=== SOW SCOPE (USE AS PRIMARY SOURCE) ===\n" + sow_analysis[:1800] + "\n\n"
    "=== WBS — SPRINT PLAN (aligned to SOW deliverables) ===\n" + wbs_for_prompt + "\n\n"
    "=== PATTERNS FROM PAST PROJECTS (reference only) ===\n" + past_patterns[:1200] + "\n\n"
    "Write EXACTLY these 7 sections using the EXACT ## headers below:\n\n"
    "## EXECUTIVE SUMMARY\n"
    "3 paragraphs: (1) what SOW deliverables this plan covers and business value for "
    + customer_name + ", (2) how past project patterns reduce risk, (3) measurable KPIs and success criteria.\n\n"
    "## RAID LOG\n"
    "Min 10 rows. Every risk MUST reference a specific SOW deliverable or constraint.\n"
    "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
    "Format: CATEGORY | TYPE | DESCRIPTION | LIKELIHOOD | IMPACT | MITIGATION | OWNER\n"
    "CATEGORY must be: Risk, Assumption, Issue, or Dependency\n"
    "LIKELIHOOD and IMPACT must be: High, Medium, or Low\n\n"
    "## TEST CASES\n"
    "Min 10 rows. Each test must map to a SOW acceptance criterion.\n"
    "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
    "Format: ID | PRIORITY | TYPE | SCENARIO | PRECONDITIONS | STEPS | EXPECTED RESULT\n"
    "PRIORITY must be: High, Medium, or Low. TYPE must be: Integration, Data Quality, Security, Performance, Functional, Regression, or UAT\n\n"
    "## DELIVERY TIMELINE\n"
    "One row per sprint/milestone from the WBS. Tie each milestone to a SOW deliverable.\n"
    "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
    "Format: WEEK | SPRINT/PHASE | MILESTONE | OWNER | STATUS | RISK FLAG\n\n"
    "## DEPLOYMENT STRATEGY\n"
    "Prose only. Phased go-live plan tied to SOW acceptance criteria. Rollback plan. Sign-off gates. "
    "Reference specific deliverables by name.\n\n"
    "## SOW TRACEABILITY MATRIX\n"
    "One row per SOW deliverable. Every deliverable from the SOW must appear here.\n"
    "CRITICAL: Output ONLY pipe-delimited data rows — NO header row, NO bullets, NO blank lines between rows.\n"
    "Format: SOW REQUIREMENT | SPRINT/PHASE | KEY TASKS | ACCEPTANCE CRITERIA | STATUS\n\n"
    "## LESSONS FROM PAST PROJECTS\n"
    "Bullet list. Each lesson must state: what happened in a past project and how it changed THIS plan.\n\n"
    "ABSOLUTE RULE: Sections with pipe-delimited format must contain ONLY data rows. "
    "No column headers, no section sub-titles, no bullet points, no blank lines within them."
)
```
### Purpose:
Assembles the final project plan from the SOW analysis, WBS, and past project patterns. It produces seven sections in a precise format (executive summary, RAID log, test cases, delivery timeline, deployment strategy, SOW traceability matrix, lessons from past projects). The prompt enforces traceability back to the SOW and uses pipe-delimited tables for structured data.

### 6. Risk Extractor Prompt (in Risk Analyser tab)
```
Extract all risks from this plan for "{plan_name}".
Plan:
{plan[:2500]}

Return ONLY valid JSON array:
[{{"risk":"title","type":"Technical/Resource/Timeline/Budget/Compliance",
"likelihood":"High/Medium/Low","impact":"High/Medium/Low",
"mitigation":"one sentence","owner":"role"}}]
```

### Purpose:
Extracts risks from the generated plan and returns them as a structured JSON array. This is used to populate the “Risk Analyser” tab, where users can view and score risks.

## 7. Q&A Prompt (in Ask the Plan tab)
```
(
    "You are a senior PMO consultant. Answer the question using the project context.\n\n"
    "Project: " + plan_name + "\n"
    "SOW Summary:\n" + sow_ctx + "\n\n"
    "WBS Summary:\n" + wbs_ctx + "\n\n"
    "Plan (excerpt):\n" + plan[:1800] + "\n\n"
    "Question: " + q + "\n\n"
    "Answer specifically and practically. "
    "If the question asks for a document or template, produce it fully."
)
```
### Purpose:
Answers user‑posed questions about the generated project plan, SOW analysis, and WBS. It provides a conversational interface for clarifying plan details, generating emails, or extracting additional insights.

