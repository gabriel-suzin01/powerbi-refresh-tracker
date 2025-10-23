# 🕰️ Power BI Refresh Tracker

This project does API calls to get the *scheduled refreshes* for Power BI Online **semantic models**.  
The data is collected and transferred to an `.xlsx` file, which is then published to **SharePoint** and used in a **Power BI report**!

### 👣 Installation & Execution

- **Just access the `releases` tab!**
- If you wish to adjust any execution settings, simply modify the values inside the `settings.ini` file.

### 🛠️ .py --> .exe Conversion

- Creating a `venv` is essential! To create a virtual environment:
```bash
    python -m venv venv_name

    venv\Scripts\activate # Windows
    # OR
    source venv/bin/activate # Linux

    pip install -r requirements.txt
```
- If you want to manually convert `main.py` to an executable, you can do so using the `pyinstaller` library. Use the following command to include all imports and dependencies correctly:  
```bash
pyinstaller --onefile --noconsole --paths=./folder_name --hidden-import=setup --hidden-import=common --hidden-import=info --hidden-import=sharepoint --hidden-import=__init__ main.py
```

### 📝 Code Quality

###### _All scripts were checked using the `pylint` library (version 3.3.8) and received the maximum score (10/10)!_ ✅
