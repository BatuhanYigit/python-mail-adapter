import os
import re
import requests
import zipfile
import pandas as pd
from exchangelib import Credentials, Account, DELEGATE, Message, HTMLBody
from dotenv import load_dotenv
from db import (
    create_tables,
    get_db,
    add_flight,
    add_flights_bulk,
    delete_min_date_max_date,
    check_mail_id,
)
import datetime

load_dotenv()

email = os.getenv("EMAIL_ADDRESS")
password = os.getenv("EMAIL_PASSWORD")

credentials = Credentials(email, password)
account = Account(
    email, credentials=credentials, autodiscover=True, access_type=DELEGATE
)

inbox = account.inbox

# print("MAILBOX = ", inbox)

sender_email = os.getenv("SENDER_EMAIL")
subject_keyword = os.getenv("SUBJECT_KEYWORD")

# Zipten Çıkartma fonksiyonu


def extract_zip(file_path, extract_to):
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
        print(f"Extracted zip {extract_to}")


# Mail okuma fonksiyonu
def mark_read_mail(item):
    item.is_read = True
    item.save()
    print(f"Mail '{item.subject}' readed")


# Csv Process
def process_csv(file_path):
    df = pd.read_csv(file_path)
    df["Time series"] = pd.to_datetime(df["Time series"])
    flights_data = []
    for _, row in df.iterrows():
        flight_data = {
            "OriginCountryCode": row["DepIATACtry"],
            "OriginCityCode": row["DepCity"],
            "OriginAirportCode": row["DepAirport"],
            "AirlineCode": row["Carrier1"],
            "DestinationCountryCode": row["ArrIATACtry"],
            "DestinationCityCode": row["ArrCity"],
            "DestinationAirportCode": row["ArrAirport"],
            "Seat": row["Seats (Total)"],
            "Date": row["Time series"],
        }
        flights_data.append(flight_data)

    return flights_data


# Find min date max date
def find_min_date_max_date(flights_data):
    df = pd.DataFrame(flights_data)
    df["Date"] = pd.to_datetime(df["Date"])

    result = df.groupby("OriginCountryCode")["Date"].agg(["min", "max"]).reset_index()
    for _, row in result.iterrows():
        print(
            f"Ülke: {row['OriginCountryCode']}, Min Tarih: {row['min']}, Max Tarih: {row['max']}"
        )


def main():

    create_tables()

    for item in inbox.filter(subject__contains=subject_keyword).order_by(
        "datetime_received"
    ):
        print(
            f"İtem Detayları: {{'subject': '{item.subject}', 'sender': '{item.sender.email_address}', 'Okundu : '{item.is_read}}}"
        )
        if isinstance(item, Message) and item.sender.email_address == sender_email:
            if item.is_read == False:
                print(
                    f"İtem Detayları: {{'subject': '{item.subject}', 'sender': '{item.sender.email_address}', 'body': '{item.body}'}}"
                )

                match = re.search(
                    r"https://analyserschedoutbound\.blob\.core\.windows\.net/[^\s]+\.zip",
                    item.body,
                )
                if match:
                    link = match.group(0)
                    print(f"Link found: {link}")

                    response = requests.get(
                        link, verify=False
                    )  # SSL SERTİFİKA DEVRE DIŞI BIRAK
                    filename = link.split("/")[-1]
                    with open(filename, "wb") as f:
                        f.write(response.content)
                    print(f"File {filename} successfully downloaded.")

                    print("Mail iddd : ", item.id)

                    mail_id = item.id

                    # mail_id = filename.split("_")[-1].replace(".zip", "")

                    extract_to = "."
                    os.makedirs(extract_to, exist_ok=True)
                    extract_zip(filename, extract_to)

                    # csv_file = [
                    #     f for f in os.listdir(extract_to) if f.endswith(".csv")
                    # ][0]

                    csv_file = filename.replace(".zip", ".csv")

                    print("csvvvvvvvv == ", csv_file)

                    start_process = datetime.datetime.now()

                    print(f"proccess işlemi başladı zaman = {start_process} ")

                    flights_data = process_csv(csv_file)

                    finish_process = datetime.datetime.now() - start_process

                    print(f"proccess işlemi bitti zaman = {datetime.datetime.now()} ")

                    print(f"proccess işlemi bitti toplam zaman {finish_process} ")
                    db = next(get_db())

                    if check_mail_id(db, mail_id):
                        print("Mail already process continue ..")
                        continue

                    print("Start delete time = ", datetime.datetime.now())
                    delete_min_date_max_date(db, finish_process, flights_data, mail_id)
                    print("Fİnish delete time = ", datetime.datetime.now())

                    start_db_insert = datetime.datetime.now()

                    print(f"İnsert db start time = {start_db_insert}")
                    add_flights_bulk(db, finish_process, mail_id, flights_data)
                    finish_db_insert = datetime.datetime.now()
                    print(f"Finish insert db time = {finish_db_insert}")
                    total_insert_db_time = datetime.datetime.now() - start_db_insert
                    print(f"Finish total db insert time = {total_insert_db_time}")

                    mark_read_mail(item)
                else:
                    print("No link found in the email body.")
            else:
                print(
                    f"Email from {item.sender.email_address} does not match the specified sender."
                )


if __name__ == "__main__":
    main()
