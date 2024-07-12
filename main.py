import os
import re
import requests
from exchangelib import Credentials, Account, DELEGATE, Message, HTMLBody
from dotenv import load_dotenv

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

for item in inbox.filter(subject__contains=subject_keyword):
    print(
        f"İtem Detayları: {{'subject': '{item.subject}', 'sender': '{item.sender.email_address}', 'body': '{item.body}'}}"
    )
    # if isinstance(item, Message) and item.sender.email_address == sender_email:
    #     print(
    #         f"İtem Detayları: {{'subject': '{item.subject}', 'sender': '{item.sender.email_address}', 'body': '{item.body}'}}"
    #     )

    #     match = re.search(
    #         r"https://analyserschedoutbound\.blob\.core\.windows\.net/[^\s]+\.zip",
    #         item.body,
    #     )
    #     if match:
    #         link = match.group(0)
    #         print(f"Link found: {link}")

    #         # SSL SERTİFİKA DEVRE DIŞI BIRAK
    #         response = requests.get(link, verify=False)
    #         filename = link.split("/")[-1]
    #         with open(filename, "wb") as f:
    #             f.write(response.content)
    #         print(f"File {filename} successfully downloaded.")
    #     else:
    #         print("No link found in the email body.")
    # else:
    #     print(
    #         f"Email from {item.sender.email_address} does not match the specified sender."
    #     )

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
