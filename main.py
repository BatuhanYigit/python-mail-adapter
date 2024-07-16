import os
import re
import requests
import zipfile
import pandas as pd
from exchangelib import Credentials, Account, DELEGATE, Message, HTMLBody
from dotenv import load_dotenv
from db import create_tables, get_db, add_flight, add_flights_bulk

load_dotenv()

email = os.getenv("EMAIL_ADDRESS")
password = os.getenv("EMAIL_PASSWORD")

credentials = Credentials(email, password)
account = Account(
    email, credentials=credentials, autodiscover=True, access_type=DELEGATE
)

inbox = account.inbox

print("MAILBOX = ", inbox)

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


# csv dönüştürme
# def process_csv(file_path):
#     df = pd.read_csv(file_path)
#     db = next(get_db())
#     for _, row in df.iterrows():
#         flight_data = {
#             "OriginCountryCode": row["DepIATACtry"],
#             "OriginCityCode": row["DepCity"],
#             "OriginAirportCode": row["DepAirport"],
#             "AirlineCode": row["Carrier1"],
#             "DestinationCountryCode": row["ArrIATACtry"],
#             "DestinationCityCode": row["ArrCity"],
#             "DestinationAirportCode": row["ArrAirport"],
#             "Seat": row["Seats (Total)"],
#             "Date": row["Time series"],
#         }
#         add_flight(db, flight_data)
#     print(f"First data insert data from {file_path}")


def process_csv(file_path):
    df = pd.read_csv(file_path)
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


def main():

    create_tables()

    for item in inbox.filter(subject__contains=subject_keyword):
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

                    # SSL SERTİFİKA DEVRE DIŞI BIRAK
                    response = requests.get(link, verify=False)
                    filename = link.split("/")[-1]
                    with open(filename, "wb") as f:
                        f.write(response.content)
                    print(f"File {filename} successfully downloaded.")

                    extract_to = "."
                    os.makedirs(extract_to, exist_ok=True)
                    extract_zip(filename, extract_to)

                    csv_file = [
                        f for f in os.listdir(extract_to) if f.endswith(".csv")
                    ][0]
                    flights_data = process_csv(csv_file)

                    add_flights_bulk(flights_data)

                    mark_read_mail(item)
                else:
                    print("No link found in the email body.")
            else:
                print(
                    f"Email from {item.sender.email_address} does not match the specified sender."
                )

        # Sertifika indirme
        #     if match:
        #         if match:
        #             link = match.group(0)
        #             print(f"Link found: {link}")

        #
        #             response = requests.get(link, verify="/path/to/your/certificate.pem")
        #             filename = link.split("/")[-1]
        #             with open(filename, "wb") as f:
        #                 f.write(response.content)
        #             print(f"File {filename} successfully downloaded.")
        #         else:
        #             print("No link found in the email body.")
        # else:
        #     print(
        #         f"Email from {item.sender.email_address} does not match the specified sender."
        #     )


if __name__ == "__main__":
    main()
