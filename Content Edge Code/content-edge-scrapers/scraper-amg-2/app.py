import asyncio
from datetime import date
import logging
import json
import os
from dotenv import load_dotenv
import boto3

load_dotenv()

# --- Normaliser ---
from normalise import clean_data

# --- Scraper Imports ---
from KKR_Global_Corporate import KKRGLOBALCO
from Nuveen_Investments_United_States_Financial_Professional import NuveenUSFA as Nuveen_amg

from pimco_singapore_financial_intermediary import PIMCOSGFI
from pimco_united_kingdom_financial_professional import PIMCOUKFP
from pimco_united_states_financial_advisors import PIMCOUSFA

from schroders_singapore_wealth_management import SIMSGWM
from schroders_united_kingdom_intermediary import SIMUKI
from schroders_united_states_intermediary import SIMUSI

from alliance_united_kingdom_financial_intermediary import ABUKFI
from alliance_united_kingdom_financial_professional import ABUKFP
from alliance_united_states_financial_professional import ABUSFP

from robeco_united_kingdom_corporate import RobecoUKC
from robeco_united_states_corporate import RobecoUSC
from robeco_singapore_corporate import RobecoSGC

from bnp_singapore_financial_intermediary import BNPSGFI
from bnp_united_kingdom_financial_intermediary import BNPUKFI

from ssga_united_kingdom_financial_professional import SSGAUKFP
from ssga_united_states_financial_professional import SSGAUSFP

from franklin_singapore_financial_professional import FTSGFP
from franklin_united_kingdom_financial_professional import  FTUKFP
from franklin_united_states_financial_professional import FTUSFP

from blackstone_united_states_corporate import BSUSCO

from Ares_Global_Corporate import ARESGCO

from allspring_singapore_financial_Intermediary import ASGISGFI
from allspring_united_Kingdom_financial_Intermediary import ASGIUKFI
from allspring_united_states_financial_advisor import ASGIUSFA

from wellington_united_kingdom_financial_professional import WMGUKFP
from wellington_singapore_financial_intermediary import WMGSGFP
from wellington_united_states_financial_intermediary import WMGUSFI

from federated_united_states_financial_advisor import FHUSFA

from jp_morgan_united_states_financial_professional import JPMUSFP
from jp_morgan_united_Kingdom_financial_advisor import JPMUKFA

from gsami_singapore_financial_intermediary import GSAMSGFI
from gsami_united_states_financial_intermediary import GSAMUSFI
from gsami_united_kingdom_financial_intermediary import GSAMUKFI
# --- AWS Setup ---
s3 = boto3.resource("s3")
lambda_client = boto3.client("lambda")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("LAMBDA_HANDLER")
bucket_name = os.getenv("BUCKET_NAME")


def save_json_to_s3(data, bucket_name, file_key):
    try:
        client = boto3.client("s3")
        json_data = json.dumps(data, indent=2, default=str)

        client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data,
            ContentType="application/json",
        )
        return True

    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        return False


def lambda_handler(event, context):
    company_site_id = event.get("company_site_id")
    target_date = event.get("target_date", str(date.today()))

    match company_site_id:

        case "am-213":
            logger.info("am-213 | KKR Global Corporate")
            scraper_func = KKRGLOBALCO

        case "am-201":
            logger.info("am-201 | Nuveen US Financial Professional")
            scraper_func = Nuveen_amg

        case "am-212":
            logger.info("am-212 | PIMCO SG Financial Intermediary")
            scraper_func = PIMCOSGFI

        case "am-211":
            logger.info("am-211 | PIMCO UK Financial Professional")
            scraper_func = PIMCOUKFP

        case "am-210":
            logger.info("am-210 | PIMCO US Financial Advisors")
            scraper_func = PIMCOUSFA

        case "am-231":
            logger.info("am-231 | Schroders SG Wealth Management")
            scraper_func = SIMSGWM

        case "am-230":
            logger.info("am-230 | Schroders UK Intermediary")
            scraper_func = SIMUKI

        case "am-229":
            logger.info("am-229 | Schroders US Intermediary")
            scraper_func = SIMUSI

        case "am-291":
            logger.info("am-291 | AB UK Financial Intermediary")
            scraper_func = ABUKFI

        case "am-290":
            logger.info("am-290 | AB UK Financial Professional")
            scraper_func = ABUKFP

        case "am-289":
            logger.info("am-289 | AB US Financial Professional")
            scraper_func = ABUSFP

        case "am-233":
            logger.info("am-233 | Robeco UK Corporate")
            scraper_func = RobecoUKC

        case "am-232":
            logger.info("am-232 | Robeco US Corporate")
            scraper_func = RobecoUSC

        case "am-234":
            logger.info("am-234 | Robeco SG Corporate")
            scraper_func = RobecoSGC

        case "am-239":
            logger.info("am-239 | BNP SG Financial Intermediary")
            scraper_func = BNPSGFI

        case "am-238":
            logger.info("am-238 | BNP UK Financial Intermediary")
            scraper_func = BNPUKFI

        case "am-265":
            logger.info("am-265 | SSGA UK Financial Professional")
            scraper_func = SSGAUKFP

        case "am-264":
            logger.info("am-264 | SSGA US Financial Professional")
            scraper_func = SSGAUSFP

        case "am-280":
            logger.info("am-280 | Franklin SG Financial Professional")  
            scraper_func = FTSGFP
        case "am-279":
            logger.info("am-279 | Franklin UK Financial Professional")  
            scraper_func = FTUKFP
        case "am-278":
            logger.info("am-278 | Franklin US Financial Professional")  
            scraper_func = FTUSFP
        
        case "am-272":
            logger.info("am-272 | Blackstone US Corporate") 
            scraper_func = BSUSCO
        
        case "am-353":
            logger.info("am-353 | Ares Global Corporate")
            scraper_func = ARESGCO
        
        case "am-340":
            logger.info("am-340 | Allspring Singapore Financial Intermediary")
            scraper_func = ASGISGFI

        case "am-339":
            logger.info("am-339 | Allspring United Kingdom Financial Intermediary")
            scraper_func = ASGIUKFI
        
        case "am-338":
            logger.info("am-338 | Allspring United States Financial Advisor")
            scraper_func = ASGIUSFA

        case "am-297":
            logger.info("am-297 | Wellington Singapore Financial Intermediary")
            scraper_func = WMGSGFP
        case "am-296":
            logger.info("am-296 | Wellington United Kingdom Financial Professional")
            scraper_func = WMGUKFP
        case "am-295":
            logger.info("am-295 | Wellington United States Financial Intermediary")
            scraper_func = WMGUSFI
        
        case "am-218":
            logger.info("am-218 | Federated Hermes United States Financial Advisor")
            scraper_func = FHUSFA

        case "am-247":
            logger.info("am-247 | J.P. Morgan Asset Management United States Financial Professional")
            scraper_func = JPMUSFP
        case "am-248":
            logger.info("am-248 | J.P. Morgan Asset Management United Kingdom Financial Advisor")
            scraper_func = JPMUKFA
        
        case "am-271":
            logger.info("am-271 | Goldman Sachs AM International Singapore Financial Intermediary")
            scraper_func = GSAMSGFI
        case "am-270":  
            logger.info("am-270 | Goldman Sachs AM International United States Financial Intermediary")
            scraper_func = GSAMUSFI
        case "am-273":
            logger.info("am-273 | Goldman Sachs AM International United Kingdom Financial Intermediary")
            scraper_func = GSAMUKFI
        
        case "am-264":  
            logger.info("am-264 | State Street Global Advisors United States Financial Professional")
            scraper_func = SSGAUSFP

        case "am-265":
            logger.info("am-265 | State Street Global Advisors United Kingdom Financial Professional")
            scraper_func = SSGAUKFP
            




        case _:
            logger.error(f"Unknown company_site_id: {company_site_id}")
            return {"statusCode": 400, "body": "Unknown company_site_id"}

    response = asyncio.run(scraper_func(target_date))

    if response == 200:
        output_path = f"/tmp/{company_site_id}.json"

        with open(output_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        data = clean_data(data)

        target_date_str = "/".join(target_date.split("-"))
        file_key = f"output/website/{target_date_str}/{company_site_id}.json"

        save_json_to_s3(data, bucket_name, file_key)

    return response
