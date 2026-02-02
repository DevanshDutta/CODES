import asyncio
from datetime import date
import logging
import json

import os
from dotenv import load_dotenv
load_dotenv()


from normalise import clean_data
import uuid
import boto3

s3 = boto3.resource('s3')
lambda_client = boto3.client('lambda')

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("BNYM_LAMBDA")



from bnymellon_united_kingdom_financial_advisor import BNYMIMUKFA

from bnymellon_united_states_financial_advisor import BNYMIMUSFA

from vanguard_united_states_financial_advisor import VanguardUSFA

from vanguard_united_kingdom_professional_investor import VanguardUKPI

from capital_united_states_financial_professional import CapitalUSFP

from capital_united_kingdom_financial_professional import CapitalUKFP

from capital_singapore_financial_professional import CapitalSGFP

from apollo_global_wealth_professional import ApolloGlobalWM

from invesco_united_kingdom_financial_professional import InvescoUKFA

from invesco_united_states_financial_professional import InvescoUSFP

from blackrock_united_states_financial_professional import BlackRockUSFP

from ubs_united_states_financial_advisor import UBSUSFA

from ubs_united_kingdom_financial_advisor import UBSUKFA

from axa_united_states_corporate import AxaUSCO

from axa_united_kingdom_corporate import AxaUKCO

from axa_singapore_corporate import AxaSGCO

from msim_united_states_financial_professional import MSIMUSFP

from msim_united_kingdom_financial_professional import MSIMUKFP

from mandg_united_kingdom_financial_professional import MANDGUKFP

from mandg_singapore_financial_professional import MANDGSGFP

from fidelity_global_financial_advisor import FidelityGlobalFA

from landg_united_kingdom_wealth_advisor import LANDGWMUK

from landg_singapore_wealth_advisor import LANDGWMSG

from natixisim_united_states_financial_professional import NatixisUSFP

from natixisim_united_kingdom_financial_professional import NatixisUKFP

from natixisim_singapore_financial_professional import NatixisSGFP

from allianz_united_kingdom_wealth_manager import AllianzUKWM

from allianz_singapore_wealth_manager import AllianzSGWM

from trowe_united_kingdom_financial_professional import TrowepriceUKFP

from trowe_singapore_financial_professional import TrowepriceSGFP

bucket_name=os.getenv("BUCKET_NAME")


def save_json_to_s3(data, bucket_name, file_key):
    try:
        s3 = boto3.client('s3')
        json_data = json.dumps(data, indent=2, default=str)
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data,
            ContentType='application/json'
        )
        
        return f"Successfully saved JSON to s3://{bucket_name}/{file_key}"
        
    except Exception as e:
        return f"Error saving JSON to S3: {e}"


def lambda_handler(event, context):
    data=[]
    company_site_id=event.get("company_site_id")
    target_date=event.get("target_date",str(date.today()))

    match company_site_id:

        case "am-255":
            logger.info("am-255 | BNY Mellon Investment Management | US | Financial Advisor")
            scraper_func = BNYMIMUSFA
        
        case "am-256":
            logger.info("am-256 | BNY Mellon Investment Management | UK | Financial Advisor")
            scraper_func= BNYMIMUKFA

        case "am-205":
            logger.info("am-205 | Vanguard | US | Financial Advisor")
            scraper_func= VanguardUSFA

        case "am-206":
            logger.info("am-206 | Vanguard | UK | Professional Investor")
            scraper_func= VanguardUKPI

        case "am-223":
            logger.info("am-223 | Capital Group | US | Financial Professional")
            scraper_func=CapitalUSFP
        
        case "am-224":
            logger.info("am-224 | Capital Group | UK | Financial Professional")
            scraper_func= CapitalUKFP

        case "am-225":
            logger.info("am-225 | Capital Group | SG | Financial Professional")
            scraper_func = CapitalSGFP             

        case "am-215":
            logger.info("am-215 | Apollo Global Management | Global | Wealth Professional")
            scraper_func = ApolloGlobalWM
        case "am-301":
            logger.info("am-301 | Invesco | US | Financial Professional")
            scraper_func= InvescoUSFP 
        
        case "am-302":
            logger.info("am-302 | Invesco | UK | Financial Professional")
            scraper_func = InvescoUKFA

        case "am-243":
            logger.info("am-243 | BlackRock | US | Financial Professional")
            scraper_func = BlackRockUSFP

        case "am-259":
            logger.info("am-259 | UBS Asset Management | US | Financial Advisor")
            scraper_func = UBSUSFA
        
        case "am-260":
            logger.info("am-260 | UBS Asset Management | UK | Financial Advisor")
            scraper_func =UBSUKFA

        case "am-249":
            logger.info("am-259 | AXA Investment Managers | US | Corporate")
            scraper_func= AxaUSCO

        case "am-250":
            logger.info("am-250 | AXA Investment Managers | UK | Corporate")
            scraper_func =AxaUKCO

        case "am-251":
            logger.info("am-251 | AXA Investment Managers | SG | Corporate")
            scraper_func=AxaSGCO

        case "am-319":
            logger.info("am-319 | Morgan Stanley Investment Management | US | Financial Professional")
            scraper_func=MSIMUSFP

        case "am-320":
            logger.info("am-320 | Morgan Stanley Investment Management | UK | Financial Professional")
            scraper_func= MSIMUKFP

        case "am-313":
            logger.info("am-313 | M&G Investments | UK | Financial Professional")
            scraper_func=MANDGUKFP

        case "am-314":
            logger.info("am-314 | M&G Investments | SG | Financial Professional")
            scraper_func=MANDGSGFP

        case "am-274":
            logger.info("am-274 | Fidelity International | Global | Financial Advisor")
            scraper_func=FidelityGlobalFA

        case "am-350":
            logger.info("am-351 | Legal & General Investment Management | United Kingdom | Wealth Manager")
            scraper_func=LANDGWMUK

        case "am-351":
            logger.info("am-351 | Legal & General Investment Management | Asia ex-Japan | Wealth Manager")
            scraper_func=LANDGWMSG

        case "am-306":
            logger.info("am-306 | Natixis Investment Managers | United States | Financial Professional")
            scraper_func=NatixisUSFP

        case "am-307":
            logger.info("am-307 | Natixis Investment Managers | United Kingdom | Financial Professional")
            scraper_func=NatixisUKFP

        case "am-308":
            logger.info("am-308 | Natixis Investment Managers | Singapore | Financial Professional")
            scraper_func=NatixisSGFP

        case "am-324":
            logger.info("am-324 | Allianz Global Investors | United Kingdom | Wealth Manager")
            scraper_func= AllianzUKWM

        case "am-325":
            logger.info("am-325 | Allianz Global Investors | Singapore | Wealth Manager")
            scraper_func= AllianzSGWM

        case "am-345":
            logger.info("am-345 | T. Rowe Price | United Kingdom | Financial Professional")
            scraper_func=TrowepriceUKFP

        case "am-346":
            logger.info("am-346 | T. Rowe Price | Singapore | Financial Professional")
            scraper_func= TrowepriceSGFP

        case _:          
            logger.error(f"Unknown company_site_id: {company_site_id}")
            return {"statusCode": 400, "body": "Unknown company_site_id"}

    response = asyncio.run(scraper_func(target_date))
    if response==200:
        output_path=f"/tmp/{company_site_id}.json"
        with open(output_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        data=clean_data(data)
        target_date_str=("/").join(target_date.split("-"))
        file_key=f"output/website/{target_date_str}/{company_site_id}.json"
        save_json_to_s3(data, bucket_name, file_key)

    return response
