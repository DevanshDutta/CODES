import asyncio
from datetime import date
import logging
import json
import os
from unittest import case
from dotenv import load_dotenv
import boto3

load_dotenv()

# --- Normaliser ---
from normalise import clean_data

# --- Scraper Imports ---
from metlife_investment_management import (METLIFEIMCO)

from rbc_singapore_global_asset_management import (RBCSGIP)
from rbc_united_kingdom_global_asset_management import (RBCUKIP)
from rbc_united_state_global_asset_management import (RBCUSIM)

from aberdeen_united_states_financial_advisor import (ABUSFA)
from aberdeen_united_kingdom_intermediary import (ABUKI)
from aberdeen_singapore_intermediary import (ABSGI)

from baillie_gifford_united_state_intermediary import (BGUSI)
from baillie_gifford_united_kingdom_intermediary import (BGUKI) 

from pinebridge_investments_singapore_intermediary import (PBISGI)
from pinebridge_investments_united_kingdom_intermediary import (PBIUKI)
from pinebridge_investments_united_state_intermediary import (PBIUSI)

from dimensional_fund_advisors_singapore_finance_professional import (DFASGFP)
from dimensional_fund_advisors_united_kingdom_finance_professional import (DFAUKFP)
from dimensional_fund_advisors_united_state_finance_professional import (DFAUSFP)

from charles_schwab_investment_management_global_corporate import (CSIMGC)

from janus_henderson_investors_united_kingdom_financial_professional import (JHIUKFP)
from janus_henderson_investors_united_state_financial_professional import (JHIUSFP)

from pgim_singapore_intermediary import (PGIMSGI)
from pgim_united_kingdom_intermediary import (PGIMUKI)
from pgim_united_state_intermediary import (PGIMUSI)

from mfs_investment_management_united_kingdom_investment_professional import (MFSUKIP)
from mfs_investment_management_united_state_investment_professional import (MFSUSIP)

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
     
        case "am-400":
            logger.info("am-400 | MetLife Investment Management")
            scraper_func = METLIFEIMCO

        case "am-406":
            logger.info("am-406 | RBC Singapore Global Asset Management")
            scraper_func = RBCSGIP
        case "am-404":
            logger.info("am-404 | RBC United States Global Asset Management")
            scraper_func = RBCUSIM
        case "am-405":
            logger.info("am-405 | RBC United Kingdom Global Asset Management")
            scraper_func = RBCUKIP

        case "am-407":
            logger.info("am-407 | Aberdeen United States Financial Advisor")
            scraper_func = ABUSFA
        case "am-408":
            logger.info("am-408 | Aberdeen United Kingdom Intermediary")
            scraper_func = ABUKI
        case "am-409":
            logger.info("am-409 | Aberdeen Singapore Intermediary")
            scraper_func = ABSGI
        
        case "am-411":
            logger.info("am-411 | Baillie Gifford United States Intermediary")
            scraper_func = BGUSI
        case "am-412":
            logger.info("am-412 | Baillie Gifford United Kingdom Intermediary")
            scraper_func = BGUKI

        case "am-414":
            logger.info("am-414 | PineBridge Investments Singapore Intermediary")
            scraper_func = PBISGI
        case "am-415":
            logger.info("am-415 | PineBridge Investments United Kingdom Intermediary")
            scraper_func = PBIUKI
        case "am-416":
            logger.info("am-416 | PineBridge Investments United States Intermediary")
            scraper_func = PBIUSI

        case "am-418":
            logger.info("am-421 | Dimensional Fund Advisors Singapore Finance Professional")
            scraper_func = DFASGFP
        case "am-419":
            logger.info("am-420 | Dimensional Fund Advisors United Kingdom Finance Professional")
            scraper_func = DFAUKFP
        case "am-420":
            logger.info("am-419 | Dimensional Fund Advisors United States Finance Professional")
            scraper_func = DFAUSFP      

        case "am-424":
            logger.info("am-424 | Charles Schwab Investment Management Global Corporate")
            scraper_func = CSIMGC

        case "am-427":
            logger.info("am-427 | Janus Henderson Investors United Kingdom Financial Professional")
            scraper_func = JHIUKFP
        case "am-426":
            logger.info("am-426 | Janus Henderson Investors United States Financial Professional")
            scraper_func = JHIUSFP
            
        case "am-430":
            logger.info("am-430 | PGIM Singapore Intermediary")
            scraper_func = PGIMSGI
        case "am-429":  
            logger.info("am-429 | PGIM United Kingdom Intermediary")
            scraper_func = PGIMUKI
        case "am-428":
            logger.info("am-428 | PGIM United States Intermediary")
            scraper_func = PGIMUSI
            
        case "am-433":
            logger.info("am-433 | MFS Investment Management United Kingdom Investment Professional")
            scraper_func = MFSUKIP
        case "am-432":
            logger.info("am-432 | MFS Investment Management United States Investment Professional")
            scraper_func = MFSUSIP
            

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
