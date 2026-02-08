# AI-Powered-Excel-From-Static-Export-to-Live-Integration

This repository contains the source code for the "AI Operating System in Excel" project. While our previous video focused on using openpyxl to push static data into Excel files through memory buffers, this project demonstrates a dynamic, live integration inside Excel Workbook using PyXLL.
By embedding an in-workbook Sidebar implementation and an AI-Assistant directly into Excel, we move from "running scripts" to "building interactive financial software."

| Feature      | openpyxl (Static)                  | PyXLL (Dynamic)                                      |
|--------------|------------------------------------|------------------------------------------------------|
| Workflow     | External script creates a file.    | Python runs inside the Excel process.                |
| Persistence  | Context is lost once script ends.  | The AI remembers your session and active spreadsheet.|
| Interface    | CLI / Terminal.                    | Native Sidebars and Custom Ribbon Tabs.              |
| Visuals      | Pre-generated images.              | Live-updating charts via plot().                     |



🌟 Key Features## 🌟 Key Features

### Custom Task Pane (CTP Sidebar)
A persistent Python-powered sidebar embedded directly in Excel. It enables natural language interaction and executes actions **within the active workbook**, including creating and modifying worksheet elements such as tables, charts, row entries, labels, and formulas. All operations are applied live to the open spreadsheet, not via external file generation.

### The Chat Constructor (AI Assistant)
A stateful orchestration layer responsible for managing OpenAI API interactions in the context of the current Excel session. It maintains conversational state and workbook awareness to support contextual AI-assisted interaction. While conversational and analytical capabilities are quite useful and in place, chat responses do **not yet** trigger automated executions on the spreadsheet.

### Programmatic Control Beyond Native Excel
Enables execution of computational workflows that are impractical or impossible in native Excel alone—such as instant Monte Carlo simulations with dynamic visualizations, advanced NLP tasks, and large-scale data labeling—while returning results immediately to the worksheet grid and associated charts.


🛠️ Installation & Setup
1. Prerequisites
Python 3.14
PyXLL: This project requires a PyXLL license (or trial). You can find it here:
PyXLL - The Python Add-In for Excel.
OpenAI API Key: Required for the Chat Constructor logic. (must be defined in your ENV)

3. Install Dependencies
Bash
pip install pyxll pandas matplotlib openai PySide6 pywin32 openai dotmap


4. Configure PyXLL
Add the ai_assistant.py and sidebar.py scripts to your pyxll.cfg file:


[PYXLL]
modules =
    ai_assistant
    sidebar


## 📂 Project Structure
ai_assistant.py: The core "Chat Constructor" and OpenAI API integration logic.
sidebar.py: The UI code using PySide6 and create_ctp to build the Excel sidebars.
utils/plotting: Helper functions to route Matplotlib figures to Excel ranges.

## 🚀 How to Use

- Open Microsoft Excel.
- Navigate to the **AI Assistant** button on the Dashboard spreadsheet.
- Click **Reload** to ensure PyXLL modules are loaded successfully.
- Click **Sidebar** or **AI_Assistant** buttons for the respective feature to appear.
- Navigate to a data-rich spreadsheet and enquire with the AI Assistant  
  (e.g., *“Run a sensitivity analysis on these figures and summarize the results.”*).
- Use Sidebar custom functions to execute a fully automated finance workflow incrementally.
- Enjoy while automation handles API calls to AI, in-place worksheet modifications, chart generation, and full simulations inside the workbook itself.

## 🔗 Links & Resources
Tool Used: [PyXLL](https://www.pyxll.com)
Video Tutorial: [Watch the full breakdown here](https://www.youtube.com)
Follow for More: [PyFi YouTube Channel](https://www.youtube.com/@Py_Fi)

