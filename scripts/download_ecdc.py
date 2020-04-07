import json
import dateutil
from collections import OrderedDict

import pandas as pd
import requests
import logging

logging.basicConfig()
logger = logging.getLogger("download_ecdc")

url = "https://opendata.ecdc.europa.eu/covid19/casedistribution/csv"
url_json = "https://opendata.ecdc.europa.eu/covid19/casedistribution/json"

def main():
    logger.info("Downloading csv data")
    df = pd.read_csv(url)
    logger.info("Downloading json data")
    r = requests.get(url_json)
    data_json = r.json()["records"]

    fields = OrderedDict({
        "dateRep": "date",
        "countriesAndTerritories": "authority",
        "countryterritoryCode": "alpha_3",
        "geoId": "alpha_2",
        "popData2018": "population_2018",
        "cases": "cases",
        "deaths": "deaths"
    })

    logger.info("Transforming csv data")
    df.rename(columns=fields, inplace=True)
    df = df[[v for k,v in fields.items()]]

    df["date"] = df.date.apply(
        lambda x: dateutil.parser.parse(x, dayfirst=True).date().isoformat()
    )

    logger.info("Transforming JSON data")
    def json_transform(obj):

        res = {
            v: obj[k] for k,v in fields.items()
        }

        res["date"] = dateutil.parser.parse(
            res["date"], dayfirst=True
        ).date().isoformat()

        return res

    data_json = [
        json_transform(i) for i in data_json
    ]

    logger.info("Caching csv data")
    df.to_csv("dataset/ecdc_covid19.csv", index=False)

    logger.info("Caching json data")
    with open("dataset/ecdc_covid19.json", "w+") as fp:
        json.dump(data_json, fp)




if __name__ == "__main__":
    main()
    pass
