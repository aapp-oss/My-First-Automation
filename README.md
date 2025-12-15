# My-First-Automation
This script automates the process of taking data from PDFs (donor forms, pledge confirmations, gift reports) and converting it into a standardized Excel import template that can be loaded directly into Andar. It eliminates manual data entry, reduces errors, and speeds up gift processing workflows.


Key Features

-Reads PDF files and extracts relevant text and tables.

-Parses and normalizes fields (e.g., donor name, address, ID, amounts, dates, fund, campaign).

-Maps the extracted data to the exact column layout required by your Andar import template.

-Outputs an Excel file (.xlsx) that’s ready for straight import into Andar.

-Basic validation (e.g., date formats, numeric amounts, required fields).


Inputs & Outputs

Input: One or more PDF files containing donor/gift information (e.g., scanned statements, form PDFs, reports).
Output: An Excel file matching your Andar template (e.g., andar_import_template.xlsx) populated with the parsed data.
