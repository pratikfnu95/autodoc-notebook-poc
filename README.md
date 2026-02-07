# agenticpoc-notebooks

## 📘 Overview
`agenticpoc-notebooks` is a **proof-of-concept (POC) repository** that contains sample Python and Databricks-style notebooks used to **trigger an automated documentation pipeline**.

Whenever a notebook is pushed to this repository, a GitHub webhook notifies an external agentic service (`confluxdocs-server`) which:
- Detects changed notebooks
- Uses an LLM to summarize the code and logic
- Publishes up-to-date documentation to Confluence in near real time

This repository acts purely as a **source-of-truth codebase** for testing auto-documentation workflows.

---

## 🎯 Purpose of This Repository
- Store **sample Python / Databricks notebooks**
- Simulate real-world data engineering pipelines
- Trigger GitHub webhooks on notebook changes
- Validate AI-powered documentation generation
- Test Confluence page creation and updates

> ⚠️ This repo does **not** contain webhook logic, LLM code, or Confluence integrations.  
> Those live in the companion service repo.


