from dotenv import load_dotenv
import requests
import logging
import json
import logging.handlers as handlers
import os
import sys
from dataclasses import dataclass
from http import HTTPStatus
import time
from os import environ

DEFAULT_360_API_URL = "https://{{DomainId}}.scim-api.passport.yandex.net/"
ITEMS_PER_PAGE = 100
MAX_RETRIES = 3
LOG_FILE = "change_scim_user_name.log"
RETRIES_DELAY_SEC = 2

EXIT_CODE = 1

logger = logging.getLogger("change_scim_user_name")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10* 1024 * 1024,  backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

@dataclass
class SettingParams:
    oauth_token: str
    domain_id: int  
    users_file : str
    new_login_default_format : str

def get_settings():
    exit_flag = False
    settings = SettingParams (
        oauth_token = os.environ.get("SCIM_TOKEN_ARG"),
        domain_id = os.environ.get("SCIM_DOMAIN_ID_ARG"),
        users_file = os.environ.get("USERS_FILE_ARG"),
        new_login_default_format = os.environ.get("NEW_LOGIN_DEFAULT_FORMAT_ARG"),
    )

    if not settings.oauth_token:
        logger.error("OAUTH_TOKEN_ARG is not set")
        exit_flag = True

    if settings.domain_id.strip() == "":
        logger.error("SCIM_DOMAIN_ID_ARG is not set")
        exit_flag = True

    if not settings.users_file:
        logger.error("USERS_FILE_ARG is not set")
        exit_flag = True

    if not settings.new_login_default_format:
        settings.new_login_default_format = "alias@domain.tld"
    
    if exit_flag:
        return None
    
    return settings

def main_menu(settings: "SettingParams"):

    while True:
        print("\n")
        print("-------------------------- Config params ---------------------------")
        print(f'New loginName format: {settings.new_login_default_format}')
        print("--------------------------------------------------------------------")
        print("\n")

        print("Select option:")
        print("1. Set new loginName format (default: alias@domail.tld).")
        print("2. Download current users into file.")
        print("3. Use users file to change loginName of users.")
        # print("4. Output bad records to file")
        print("0. Exit")

        choice = input("Enter your choice (0-3): ")

        if choice == "0":
            print("Goodbye!")
            break
        elif choice == "1":
            print('\n')
            set_new_loginName_format(settings)
        elif choice == "2":
            print('\n')
            download_users_to_file(settings)
        elif choice == "3":
            print('\n')
            update_users_from_file(settings)
        # elif choice == "4":
        #     analyze_data = add_contacts_from_file(True)
        #     OutputBadRecords(analyze_data)
        else:
            print("Invalid choice. Please try again.")

def set_new_loginName_format(settings: "SettingParams"):
    answer = input("Enter format of new userLogin name (space to use default format alias@domain.tld):\n")
    if answer:
        if answer.strip() == "":
            settings.new_login_default_format = "alias@domain.tld"
        else:
            settings.new_login_default_format = answer.strip()

    return settings

def download_users_to_file(settings: "SettingParams"):

    users = []
    headers = {
        "Authorization": f"Bearer {settings.oauth_token}"
    }
    url = DEFAULT_360_API_URL.replace("{{DomainId}}", settings.domain_id)
    startIndex = 1
    items = ITEMS_PER_PAGE
    try:
        retries = 1
        while True:           
            response = requests.get(f"{url}/v2/Users?startIndex={startIndex}&count={items}", headers=headers)
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during GET request: {response.status_code}. Error message: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Forcing exit without getting data.")
                    return
            else:
                retries = 1
                temp_list = response.json()["Resources"]
                logger.debug(f'Received {len(temp_list)} records.')
                users.extend(temp_list)

                if int(response.json()["startIndex"]) + int(response.json()["itemsPerPage"]) > int(response.json()["totalResults"]) + 1:
                    break
                else:
                    startIndex = int(response.json()["startIndex"]) + int(response.json()["itemsPerPage"])

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    if users:
        with open(settings.users_file, "w", encoding="utf-8") as f:
            f.write("uid;displayName;old_userName;new_userName\n")
            for user in users:
                new_userName = user["userName"]
                if "@" in user["userName"]:
                    login = user["userName"].split("@")[0]
                    domain = ".".join(user["userName"].split("@")[1].split(".")[:-1])
                    tld = user["userName"].split("@")[1].split(".")[-1]
                    new_userName = settings.new_login_default_format.replace("alias", login).replace("domain", domain).replace("tld", tld)
                f.write(f"{user['id']};{user['displayName']};{user['userName']};{new_userName}\n")
        logger.info(f"{len(users)} users downloaded to file {settings.users_file}")
    else:
        logger.info(f"No users found. Check your settings.")
        return
    return

def update_users_from_file(settings: "SettingParams"):
    user_for_change = []
    all_users = []
    with open(settings.users_file, "r", encoding="utf-8") as f:
        all_users = f.readlines()

    line_number = 1
    for user in all_users[1:]:
        line_number += 1
        if user.replace("\n","").strip():
            temp = user.replace("\n","").strip()
            try:
                uid, displayName, old_userName, new_userName = temp.split(";")
                if not any(char.isdigit() for char in uid):
                    logger.info(f"Uid {uid} is not valid ({displayName}). Skipping.")   
                    continue
                if not new_userName:
                    logger.info(f"New userName for uid {uid} ({displayName}) is empty. Skipping.")   
                    continue
                if old_userName == new_userName:
                    logger.debug(f"User {old_userName} ({displayName}) has the same new name {new_userName}. Skipping.")
                    continue
                user_for_change.append(temp)
            except ValueError as e:
                logger.error(f"Line number {line_number} has wrong count of values (should be 4 values, separated by semicolon. Skipping")

            except Exception as e:
                logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    
    if not user_for_change:
        logger.error(f"File {settings.users_file} is empty.")
        return
    else:
        for user in user_for_change:
            logger.debug(f"Will modify - {temp}.")

        answer = input(f"Modify userName SCIM attribute for {len(user_for_change)} users? (Y/n): ")
        if answer.upper() not in ["Y", "YES"]:
            return
        
    headers = {
        "Authorization": f"Bearer {settings.oauth_token}"
    }
    url = DEFAULT_360_API_URL.replace("{{DomainId}}", settings.domain_id)    
    for user in user_for_change:
        uid, displayName, old_userName, new_userName = user.strip().split(";")
        try:
            retries = 1
            while True:
                logger.info(f"Changing user {old_userName} to {new_userName}...")
                data = json.loads("""   { "Operations":    
                                            [
                                                {
                                                "value": "alias@domain.tld",
                                                "op": "replace",
                                                "path": "userName"
                                                }
                                            ],
                                            "schemas": [
                                                "urn:ietf:params:scim:api:messages:2.0:PatchOp"
                                            ]
                                        }""".replace("alias@domain.tld", new_userName))
                
                response = requests.patch(f"{url}/v2/Users/{uid}", headers=headers, json=data)
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"Error during PATCH request: {response.status_code}. Error message: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"Error. Patching user {old_userName} to {new_userName} failed.")
                        break
                else:
                    logger.info(f"Success - User {old_userName} changed to {new_userName}.")
                    break
                

        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

if __name__ == "__main__":

    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)

    settings = get_settings()

    try:
        main_menu(settings)
    except Exception as exp:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        sys.exit(EXIT_CODE)