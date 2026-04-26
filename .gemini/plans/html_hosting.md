# Migration to Public HTML Hosting

## Objective
Replace the Google Sheets integration with a lightweight solution that generates a static HTML file containing the daily stock scan data and publishes it to GitHub Pages.

## Motivation
Simplifies infrastructure by removing the need for Google Cloud Service Accounts, Google Sheets API keys, and complex authentication flows. GitHub Pages provides free, automated hosting directly from the repository.

## Implementation Steps
1. **Update `src/scanner.py`**:
   - Remove Google Sheets related code (`GoogleSheetsClient` class, `gspread` imports, etc.).
   - Modify the output logic to generate an `index.html` file using a simple template (e.g., using a basic HTML table styled with CSS) in a `public/` directory instead of writing to Google Sheets or just a local CSV.
   - Keep the CSV generation as a backup artifact if desired, but ensure `index.html` is the primary output.
2. **Update `.github/workflows/daily_scan.yml`**:
   - Remove Google Sheets and GCP secrets from the environment variables.
   - Add a step to upload the generated `public/` directory as an artifact.
   - Add a new job or steps to deploy the `public/` directory to GitHub Pages using the `actions/upload-pages-artifact` and `actions/deploy-pages` actions.
3. **Repository Configuration**:
   - The user will need to enable GitHub Pages in their repository settings, pointing the source to GitHub Actions.

## Verification
- Run `scanner.py` locally to ensure `public/index.html` is generated correctly.
- Trigger the workflow and verify the artifact is built and deployed successfully.
- Access the public GitHub Pages URL to confirm the data is visible.