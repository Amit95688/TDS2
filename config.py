# app/config.py
import os

EMAIL = os.getenv("QUIZ_EMAIL", "you@example.com")
SECRET = os.getenv("QUIZ_SECRET", "replace-with-your-secret")
TIMEOUT_SECONDS = int(os.getenv("QUIZ_TIMEOUT", "180"))  # 3 minutes default
USER_AGENT = "LLM-Analysis-Quiz-Agent/1.0"
