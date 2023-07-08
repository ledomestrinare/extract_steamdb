
import pandas as pd
import numpy as np 
from datetime import datetime
from bs4 import BeautifulSoup, element
from google.oauth2 import service_account

URL = "https://steamdb.info/sales/"

def get_html_data(URL) -> str:
    import undetected_chromedriver as uc 
    import time
    driver = uc.Chrome() 
    driver.get(URL) 
    time.sleep(10)
    driver.minimize_window()

    #selecionando para aparecer todos os registros na pagina
    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.common.by import By
    select = Select(driver.find_element(By.XPATH,"/html/body/div[4]/div[3]/div[2]/div[2]/div[2]/div[2]/div/div[1]/div[1]/label/select"))
    select.select_by_value('-1')
    html = driver.page_source
    time.sleep(1)
    driver.close()
    return html

def extract_table_rows(html_data: str) -> element.ResultSet:

    soup = BeautifulSoup(html_data, "html.parser")
    items = soup.find_all("tr", {"class": "app"})
    return items

def extract_data_from_rows(items: element.ResultSet) -> list[dict]:

    items_data = []

    for item in items:
        tds = item.findAll("td")
        name = tds[2].text.strip("\n").split("\n\n")[0]
        other_fields = tds[3:]
        other_fields_values = [field["data-sort"] for field in other_fields]
        (
            discount_in_percent,
            price_in_brl,
            rating_in_percent,
            end_time_in_seconds,
            start_time_in_second,
            release_time_in_second,
        ) = other_fields_values

        discount_in_percent = int(discount_in_percent)
        price_in_brl = float(price_in_brl) / 100
        rating_in_percent = float(rating_in_percent)
        end_time_in_seconds = int(end_time_in_seconds)
        start_time_in_second = int(start_time_in_second)
        release_time_in_second = int(release_time_in_second)

        items_data.append(
            {
                "name": name,
                "discount_in_percent": discount_in_percent,
                "price_in_brl": price_in_brl,
                "rating_in_percent": rating_in_percent,
                "end_time_in_seconds": end_time_in_seconds,
                "start_time_in_second": start_time_in_second,
                "release_time_in_second": release_time_in_second,
            }
        )

    return items_data

def build_and_export_dataframe_csv(items_data: list[dict]) -> None:

    df = pd.DataFrame(items_data)
    file_name = datetime.now().strftime("%m_%d_%Y_%H_%M_%S_steamdb.csv")
    df.to_csv(file_name, index=False)

def build_and_export_dataframe_gbq(destination, filename, if_exist, items_data: list[dict]) -> None:
    
    df = pd.DataFrame(items_data)
    credentials = service_account.Credentials.from_service_account_file('google_auth.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])
    df.to_gbq(filename, credentials=credentials, destination_table=destination, if_exists=if_exist)
    
if __name__ == "__main__":
    html_data = get_html_data(URL)
    items = extract_table_rows(html_data)
    items_data = extract_data_from_rows(items)
    build_and_export_dataframe_csv(items_data)
    #build_and_export_dataframe_gbq('dataset.dataset', 'tabela_steamdb', 'replace',items_data)
    
