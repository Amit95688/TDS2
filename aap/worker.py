# app/worker.py
import asyncio
import time
import re
import json
from typing import Optional, Dict, Any
import httpx
import pandas as pd
import pdfplumber

from playwright.async_api import async_playwright
from .utils import (
    extract_numbers, try_parse_json_from_text, decode_base64_block,
    parse_table_html_to_df, parse_csv_bytes, parse_excel_bytes, sum_dataframe_column
)
from .config import EMAIL, SECRET, TIMEOUT_SECONDS, USER_AGENT

async def fetch_bytes(http_client: httpx.AsyncClient, url: str, headers=None):
    r = await http_client.get(url, headers=headers or {}, timeout=60.0)
    r.raise_for_status()
    return r.content

async def solve_quiz_chain(start_url: str, provided_secret: str):
    """
    Visit start_url, parse the page, compute an answer, post to submit endpoint.
    Loop if server returns next 'url'. All must finish within TIMEOUT_SECONDS.
    """
    deadline = time.time() + TIMEOUT_SECONDS
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()
        http_client = httpx.AsyncClient(timeout=60.0)
        current_url = start_url
        last_response = None

        while current_url and time.time() < deadline:
            await page.goto(current_url, wait_until="networkidle")
            # grab visible text and HTML
            page_text = await page.content()
            page_plain = await page.inner_text("body")
            # Try to detect a clear JSON payload in the page
            parsed_json = try_parse_json_from_text(page_plain) or try_parse_json_from_text(page_text)

            # decode base64 block if present (common in sample)
            decoded = decode_base64_block(page_plain) or decode_base64_block(page_text)

            # generic extraction of submit URL and instructions
            submit_url = None
            submit_payload_template = None
            # heuristics: look for 'Post your answer to' followed by a URL
            m = re.search(r"(https?://[^\s'\"<>]+)", page_plain)
            if m:
                # naive pick: look for a URL that contains "submit" or "submit"
                candidates = re.findall(r"(https?://[^\s'\"<>]+)", page_plain)
                for c in candidates:
                    if "submit" in c or "submit" in c.lower() or "answer" in c.lower():
                        submit_url = c
                        break
                if not submit_url:
                    submit_url = candidates[0]

            # If parsed JSON contains answer instructions
            if parsed_json and "submit" in json.dumps(parsed_json).lower():
                # example pages often contain a JSON object with keys email, secret, url, answer
                # we will use it as a template
                submit_payload_template = parsed_json

            # If page contains explicit "Post your answer to" and a JSON snippet (like sample), extract JSON block
            jsn = try_parse_json_from_text(page_plain)
            if jsn and all(k in jsn for k in ("email", "secret", "url")):
                submit_payload_template = jsn
                submit_url = jsn.get("submit", submit_url) or jsn.get("url", submit_url)

            # Now determine question type and compute an answer
            answer = None
            answer_meta = {}

            # 1) If there's a direct instruction like "What is the sum of the 'value' column"
            q_match = re.search(r"sum of the[^\n\r]*['\"]?([A-Za-z0-9 _-]+)['\"]? column", page_plain, re.I)
            if q_match:
                colname = q_match.group(1).strip()
                # try to find a table on the page
                df = parse_table_html_to_df(page_text)
                if df is not None:
                    s = sum_dataframe_column(df, colname)
                    if s is not None:
                        answer = s
                        answer_meta["method"] = "html-table-sum"
                # else try to find and download linked files (pdf/csv/xlsx)
            # 2) If page requests to "Download file" and gives a link
            if answer is None:
                # find links to pdf/csv/xlsx
                links = re.findall(r'href=[\'"]([^\'"]+)', page_text)
                file_link = None
                for L in links:
                    if any(ext in L.lower() for ext in [".pdf", ".csv", ".xls", ".xlsx"]):
                        file_link = L
                        break
                if file_link:
                    # absolute URL handling
                    if file_link.startswith("//"):
                        file_link = "https:" + file_link
                    if file_link.startswith("/"):
                        # build from current_url
                        from urllib.parse import urljoin
                        file_link = urljoin(current_url, file_link)
                    try:
                        content = await fetch_bytes(http_client, file_link)
                        if file_link.lower().endswith(".csv"):
                            df = parse_csv_bytes(content)
                            # if column name found earlier
                            if 'colname' in locals():
                                s = sum_dataframe_column(df, colname)
                                if s is not None:
                                    answer = s
                                    answer_meta["method"] = "csv-sum"
                            else:
                                # fallback: sum first numeric column
                                for c in df.columns:
                                    try:
                                        s = float(pd.to_numeric(df[c], errors='coerce').sum())
                                        answer = s
                                        answer_meta["method"] = f"csv-first-numeric:{c}"
                                        break
                                    except Exception:
                                        continue
                        elif file_link.lower().endswith((".xls", ".xlsx")):
                            df = parse_excel_bytes(content)
                            if 'colname' in locals():
                                s = sum_dataframe_column(df, colname)
                                if s is not None:
                                    answer = s
                                    answer_meta["method"] = "excel-sum"
                            else:
                                for c in df.columns:
                                    try:
                                        s = float(pd.to_numeric(df[c], errors='coerce').sum())
                                        answer = s
                                        answer_meta["method"] = f"excel-first-numeric:{c}"
                                        break
                                    except Exception:
                                        continue
                        elif file_link.lower().endswith(".pdf"):
                            # simple approach: extract text and find numbers within a "value" column mention
                            text = ""
                            try:
                                with pdfplumber.open(io.BytesIO(content)) as pdf:
                                    for p in pdf.pages:
                                        text += "\n" + p.extract_text() or ""
                            except Exception:
                                # fallback naive decode
                                try:
                                    text = content.decode("utf-8", errors="ignore")
                                except Exception:
                                    text = ""
                            # try find table text with "value" column and sum numbers after it
                            if 'colname' in locals():
                                # gather numbers in the pdf
                                nums = extract_numbers(text)
                                if nums:
                                    answer = sum(nums)
                                    answer_meta["method"] = "pdf-sum-heuristic"
                            else:
                                nums = extract_numbers(text)
                                if nums:
                                    answer = sum(nums)
                                    answer_meta["method"] = "pdf-sum-all-numbers"
                    except Exception as e:
                        answer_meta["file_error"] = str(e)

            # 3) If page contains an explicit number answer (maybe teacher expects you to compute a simple sum given inline numbers)
            if answer is None:
                # collect all numbers on page; if prompt asks "sum" we assume sum of numbers
                if "sum of" in page_plain.lower() or "what is the sum" in page_plain.lower():
                    nums = extract_numbers(page_plain)
                    if nums:
                        answer = sum(nums)
                        answer_meta["method"] = "sum-inline-numbers"

            # 4) If the page contains a JSON template with "answer" field and maybe a file url to process, try to fill it
            if answer is None and submit_payload_template:
                # if template has url to a data file, try to fetch and analyze
                payload = submit_payload_template.copy()
                if "answer" in payload and isinstance(payload["answer"], (int, float, str)):
                    # maybe answer is already filled
                    answer = payload["answer"]
                    answer_meta["method"] = "from_template"
                elif "url" in payload and payload.get("url", "").endswith((".csv", ".pdf", ".xlsx")):
                    try:
                        c = await fetch_bytes(http_client, payload["url"])
                        if payload["url"].endswith(".csv"):
                            df = parse_csv_bytes(c)
                            # heuristic: take first numeric column sum
                            for col in df.columns:
                                try:
                                    s = float(pd.to_numeric(df[col], errors='coerce').sum())
                                    answer = s
                                    answer_meta["method"] = f"template-csv-first-num:{col}"
                                    break
                                except Exception:
                                    continue
                    except Exception:
                        pass

            # 5) If we still have no answer, try decode base64 and look for an explicit numeric answer there
            if answer is None and decoded:
                # decoded might contain JSON with "answer"
                js = try_parse_json_from_text(decoded)
                if js and "answer" in js:
                    answer = js["answer"]
                    answer_meta["method"] = "decoded-json"
                else:
                    nums = extract_numbers(decoded)
                    if nums:
                        # many sample tasks give a single intended numeric answer; sum as a fallback
                        answer = sum(nums)
                        answer_meta["method"] = "decoded-sum"

            # final fallback: if no parsed answer, set answer to a best-effort: sum of numbers in page
            if answer is None:
                nums = extract_numbers(page_plain)
                if nums:
                    answer = sum(nums)
                    answer_meta["method"] = "fallback-sum-page"

            # If still None, respond with a failure object
            if answer is None:
                # prepare a conservative response to the submit endpoint indicating "unable"
                answer_payload = {
                    "email": EMAIL,
                    "secret": provided_secret,
                    "url": current_url,
                    "answer": None,
                    "note": "unable to parse task automatically",
                }
            else:
                # construct payload
                answer_payload = {
                    "email": EMAIL,
                    "secret": provided_secret,
                    "url": current_url,
                    "answer": answer
                }
                # attach metadata (keeps under 1MB)
                answer_payload["_meta"] = {"method": answer_meta.get("method"), "raw_page_sample": None}

            # allow submit_payload_template to act as a template: merge keys but prefer computed answer
            if submit_payload_template:
                base = submit_payload_template.copy()
                base.update({k: v for k, v in answer_payload.items() if v is not None})
                answer_payload = base

            # determine target submit_url from template if present
            if submit_payload_template and isinstance(submit_payload_template, dict):
                possible = submit_payload_template.get("submit") or submit_payload_template.get("url")
                if possible:
                    submit_url = possible

            if not submit_url:
                # no submit URL discovered; bail out
                await http_client.aclose()
                await page.close()
                await context.close()
                await browser.close()
                return {"success": False, "reason": "No submit URL found on page", "page_url": current_url, "payload_tried": answer_payload}

            # POST the answer
            try:
                r = await http_client.post(submit_url, json=answer_payload, timeout=60.0)
                # accept 200 as success; if other code, include status
                try:
                    resp_json = r.json()
                except Exception:
                    resp_json = {"status_code": r.status_code, "text": r.text}
                last_response = resp_json
                # If response provides new url, follow it
                next_url = None
                if isinstance(resp_json, dict):
                    next_url = resp_json.get("url") or resp_json.get("next")
                # update for next loop
                if next_url:
                    current_url = next_url
                    continue
                else:
                    # finished chain
                    await http_client.aclose()
                    await page.close()
                    await context.close()
                    await browser.close()
                    return {"success": True, "final_response": resp_json, "submitted": answer_payload}
            except Exception as e:
                await http_client.aclose()
                await page.close()
                await context.close()
                await browser.close()
                return {"success": False, "reason": f"failed to submit answer: {str(e)}", "payload_tried": answer_payload}

        # timeout or loop exit
        await http_client.aclose()
        await page.close()
        await context.close()
        await browser.close()
        return {"success": False, "reason": "timeout or loop ended", "last_response": last_response}
