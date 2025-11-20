# GitHub Setup and Submission Guide

## Prerequisites

1. **GitHub Account**: Create one at https://github.com if you don't have one
2. **Git Installed**: Check with `git --version`

## Step-by-Step GitHub Setup

### 1. Initialize Git Repository (if not already done)

```bash
cd /Users/surya/PycharmProjects/data_engineer_assessment/aus_company_pipeline
git init
```

### 2. Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `aus_company_pipeline`
3. Description: "Data Engineering Assessment: Australian Company Data Pipeline"
4. Set to **Public**
5. **DO NOT** initialize with README (we already have one)
6. Click "Create repository"

### 3. Connect Local Repo to GitHub

```bash
# Add remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/aus_company_pipeline.git

# Verify remote
git remote -v
```

### 4. Add Files to Git

```bash
# Add all files (gitignore will exclude large data files)
git add .

# Check what will be committed
git status

# Create first commit
git commit -m "Complete Australian Company Data Pipeline

- Common Crawl extraction with WET file support
- ABR XML parsing with address extraction
- 3-tier entity matching (direct ABN, fuzzy name, LLM)
- dbt transformations and data quality tests
- Complete documentation and test suite
- Lightweight test script for MacBook"
```

### 5. Push to GitHub

```bash
# Push to main branch
git branch -M main
git push -u origin main
```

### 6. Verify Upload

1. Go to https://github.com/YOUR_USERNAME/aus_company_pipeline
2. Verify all files are there
3. Check that README.md displays nicely

## What Gets Uploaded

✅ **Included in Git:**
- All source code (`src/`, `sql/`, `scripts/`, `dbt_project/`)
- Documentation (`README.md`, `docs/`)
- Configuration files (`.env.example`, `requirements.txt`)
- Sample WARC file (`samples/sample_cc.warc.gz`)

❌ **Excluded from Git** (via .gitignore):
- Downloaded WET files (`cc_data/`)
- ABR XML files (`abr_data/*.xml`)
- Output files (`outputs/*.jsonl`)
- Database credentials (`.env`)
- Virtual environments (`venv/`, `dbt_venv/`)

## Submission

### For Assessment Submission:

**Share your repository link:**
```
https://github.com/YOUR_USERNAME/aus_company_pipeline
```

**Include in your submission email:**

```
Subject: Data Engineering Assessment Submission - [Your Name]

Repository: https://github.com/YOUR_USERNAME/aus_company_pipeline

Summary:
- Implemented complete ETL pipeline for Australian company data
- Processed Common Crawl (March 2025) and ABR data
- Built 3-tier entity matching system (55-70% expected match rate)
- Used Gemini API for intelligent matching of difficult cases
- Created dbt transformations with data quality tests
- Tested on MacBook with lightweight test suite
- All code is production-ready and documented

Technology Stack:
- Python 3.11, PostgreSQL, dbt, Gemini API
- See README for complete details

IDE: PyCharm Professional 2024.2
```

## Keeping Repository Updated

After making changes:

```bash
# Add changed files
git add .

# Commit with message
git commit -m "Description of changes"

# Push to GitHub
git push
```

## Troubleshooting

### Large File Error

If you get "file too large" error:

```bash
# Check which large files are staged
git ls-files -z | xargs -0 du -h | sort -rh | head -20

# Remove from git (keeps local file)
git rm --cached path/to/large/file

# Update .gitignore and recommit
```

### GitHub Authentication

If prompted for credentials, use **Personal Access Token** instead of password:

1. Go to https://github.com/settings/tokens
2. Generate new token (classic)
3. Select scopes: `repo`
4. Use token as password when pushing

### Alternative: GitHub Desktop

If command line is tricky, use **GitHub Desktop**:
1. Download from https://desktop.github.com/
2. Open app and sign in
3. Add existing repository
4. Commit and push via GUI

## Final Checklist

Before submission:

- [ ] README.md displays correctly on GitHub
- [ ] All code files are present
- [ ] No sensitive data (passwords, API keys) in repo
- [ ] Test script runs successfully
- [ ] Documentation is complete
- [ ] Repository is public (or accessible to assessors)
