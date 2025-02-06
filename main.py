import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

import requests
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Project setup ##################################################
BASE_URL = "https://dhcappl.nic.in/dhcorderportal"
PDF_DIR = "pdf"

FROM_YEAR = 2023
REQUIRED_CASE_TYPES = ["RFA(OS)(COMM)", "FAO(OS) (COMM)", "EFA(OS)  (COMM)", "EFA(COMM)", "RFA(COMM)", "FAO(COMM)",
                       "CS(COMM)", "O.M.P.(I) (COMM.)", "O.M.P. (E) (COMM.)",
                       "O.M.P. (J) (COMM.)", "O.M.P. (T) (COMM.)", "O.M.P. (COMM)", "ARB. A. (COMM.)",
                       "C.O. (COMM.IPD-TM)", "C.O.(COMM.IPD-CR)", "C.O.(COMM.IPD-PAT)", "C.A.(COMM.IPD-GI)",
                       "C.A.(COMM.IPD-PAT)", "C.A.(COMM.IPD-PV)", "C.A.(COMM.IPD-TM)"]

# Setting up mongo db client #####################################
client = MongoClient(os.getenv("MONGO_URI"))

db = client[os.getenv("MONGO_DB_NAME")]
collection = db[os.getenv("MONGO_COLLECTION_NAME")]


def save_to_mongodb(data):
    collection.insert_one(data)


def upload_pdf_to_azure(file_path):
    try:
        blob_name = file_path.split("/")[-1]
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_CONNECTION_STRING")
        )
        blob_client = blob_service_client.get_blob_client(
            container=os.getenv("AZURE_CONTAINER_NAME"), blob=blob_name
        )
        if blob_client.exists():
            blob_client.delete_blob()
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return blob_client.url
    except Exception as e:
        print(e)
        return None


def clean_up(path):
    if os.path.exists(path):
        os.remove(path)


def download_pdf(sess: requests.Session, pdf_url: str) -> str | None:
    try:
        file_path = os.path.join(PDF_DIR, f"{uuid.uuid4().hex}.pdf")

        url = f"{BASE_URL}/GetFile.do"
        resp = sess.post(url, data={"filepath": pdf_url}, stream=True, timeout=30)
        resp.raise_for_status()

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return file_path
    except requests.RequestException as e:
        print(f"Error downloading PDF: {e}")
        return None


def parse_order_row(row: BeautifulSoup, sess: requests.Session) -> Dict[str, str]:
    cells = row.find_all("td")
    temp = {
        "snumber": cells[0].text.strip(),
        "case_number": cells[1].text.strip(),
        "date_of_order": cells[2].text.strip(),
        "corrigenda_link": cells[3].text.strip(),
        "hindi_order": cells[4].text.strip(),
    }

    pdf_link = cells[1].find("a")
    if pdf_link and pdf_link.get("onclick"):
        pdf_path = pdf_link["onclick"].split("'")[1]

        downloaded_file_path = download_pdf(sess, pdf_path)
        if downloaded_file_path:
            temp["url"] = upload_pdf_to_azure(downloaded_file_path)
        clean_up(downloaded_file_path)
    else:
        temp["url"] = None

    return temp


def get_case_details(sess: requests.Session, ctype: str, regno: str, regyr: str, japtcha: str) -> Optional[Dict]:
    try:
        page = sess.post(f"{BASE_URL}/casetype1.do", data={
            "scode": "31",
            "fflag": "1",
            "ctype": ctype,
            "regno": regno,
            "regyr": regyr,
            "japtcha": japtcha,
        }, timeout=30)
        page.raise_for_status()
        soup = BeautifulSoup(page.content, 'html.parser')

        all_heading = soup.find_all("h5")
        if len(all_heading) < 2:
            print("Error: Unable to find required headings")
            return None

        data = {
            "parties": all_heading[0].text.strip().replace("\xa0", " "),
            "status": "",
            "next_date": "",
            "orders": []
        }

        rows = all_heading[1].find_all("span")
        if len(rows) >= 2:
            data["status"] = rows[0].text.strip()
            data["next_date"] = rows[1].text.strip()

        table = soup.find("table")
        if table:
            with ThreadPoolExecutor(max_workers=5) as executor:
                data["orders"] = list(executor.map(lambda row: parse_order_row(row, sess), table.find_all("tr")[1:]))

        return data
    except requests.RequestException as e:
        print(f"Error fetching case details: {e}")
        return None


def get_captcha(sess: requests.Session) -> Optional[str]:
    try:
        page = sess.get(f"{BASE_URL}/LaunchCaseWise.do", timeout=30)
        page.raise_for_status()
        soup = BeautifulSoup(page.content, 'html.parser')
        captcha_element = soup.find("a", {"onclick": "playAudio()"})
        return captcha_element.text.strip() if captcha_element else None
    except requests.RequestException as e:
        print(f"Error fetching captcha: {e}")
        return None


if __name__ == '__main__':
    session = requests.Session()
    captcha_code = get_captcha(session)

    if captcha_code:
        while FROM_YEAR >= 2000:
            for case_type in REQUIRED_CASE_TYPES:
                continuous_no_case = 0
                case_no = 1

                while True:
                    result = get_case_details(session, case_type, str(case_no), str(FROM_YEAR), captcha_code)

                    if not result:
                        continuous_no_case += 1
                    else:
                        result["case_info"] = f"{case_type}/{case_no}/{FROM_YEAR}"
                        save_to_mongodb(result)
                        print(f"{case_type}/{case_no}", end="\r")

                    if continuous_no_case == 20:
                        break

                    case_no += 1

            FROM_YEAR -= 1

    else:
        print("Failed to retrieve captcha")
