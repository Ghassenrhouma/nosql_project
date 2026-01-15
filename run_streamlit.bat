@echo off
REM NoSQL-LLM Streamlit Web Interface Launcher
echo 🚀 Starting NoSQL-LLM Web Interface...
cd /d %~dp0
call venv\Scripts\activate.bat
streamlit run nosql_llm_streamlit.py --server.port 8501 --server.address 127.0.0.1
pause