import schedule
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import datetime
import pdb
import numpy as np
import database as db

def job():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        url = 'https://coinmarketcap.com/currencies/bitcoin/#Markets'
        driver.get(url)
        driver.implicitly_wait(5)
        for i in range(5):
            if driver.execute_script("return document.readyState") == "complete":
                break
            else:
                time.sleep(1)

        print("----------------------------")
        timestamp = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
        for button in range(3):
            if button ==0:
                print("-+++++++ Spot ++++++-")
                spot_button = driver.find_elements(By.XPATH, '/html/body/div[1]/div[2]/div[1]/div[2]/div/div/div[6]/section/div/div[1]/div/div[2]/div[2]/button[1]')
                spot_button[0].click()
                table = driver.find_element(By.TAG_NAME, 'tbody')
                rows = table.find_elements(By.TAG_NAME,'tr')
                all_data = []
                for i in range(10):
                    temp = []
                    counter=-1
                    for paraph in rows[i].find_elements(By.TAG_NAME,'p') and rows[i].find_elements(By.TAG_NAME,'td'):
                        counter+=1
                        if counter in [3,4,5,6]:
                            value = paraph.text.replace("$", "").replace(",", "").replace("*", "")
                            float_value = float(value)
                            temp.append(float_value)
                        elif counter in [7]:
                            value = paraph.text.replace("%", "").replace(",", "").replace("*", "")
                            float_value = float(value)
                            temp.append(float_value)
                        elif counter in [9]:
                            value = paraph.text.replace(",", "").replace("*", "")
                            float_value = float(value)
                            temp.append(float_value)
                        else:
                            temp.append(paraph.text)

                    if "USD" in temp[2]:
                        all_data.append(temp)
                all_data = pd.DataFrame(all_data)

                db.connection.insertTOspot(timestamp, np.mean(all_data[3]), np.sum(all_data[4]), np.sum(all_data[5]),
                                           np.sum(all_data[6]), np.sum(all_data[7]), np.mean(all_data[9]))

            elif button == 1:
                print("-+++++++ Perpetual ++++++-")
                perpetual_button = driver.find_elements(By.XPATH, '/html/body/div[1]/div[2]/div[1]/div[2]/div/div/div[6]/section/div/div[1]/div/div[2]/div[2]/button[2]')
                perpetual_button[0].click()
                table = driver.find_element(By.TAG_NAME, 'tbody')
                rows = table.find_elements(By.TAG_NAME,'tr')
                all_data = []
                for i in range(10):                        
                    temp = []
                    counter=-1
                    for paraph in rows[i].find_elements(By.TAG_NAME,'p') and rows[i].find_elements(By.TAG_NAME,'td'):
                        counter+=1
                        if counter in [3,4,5,6,7,8,9]:                                
                            value = paraph.text.replace("$", "").replace(",", "").replace("*", "").replace("%", "").replace("--", "0").replace("<0.01", "0.001")
                            if "High" in paraph.text :
                                value = 0.001
                            try:
                                float_value = float(value)
                            except:
                                float_value = 0.001
                                
                            temp.append(float_value)
                        else:
                            temp.append(paraph.text)
                    if "USD" in temp[2]:
                        all_data.append(temp)
                
                all_data = pd.DataFrame(all_data)
                db.connection.insertTOperpetual(timestamp, np.mean(all_data[3]), np.mean(all_data[4]),
                                           np.mean(all_data[5]), np.sum(all_data[6]),np.sum(all_data[7]),
                                           np.mean(all_data[8]),np.sum(all_data[9]))
            elif button == 2:
                print("-+++++++ futures ++++++-")
                futures_button = driver.find_elements(By.XPATH, '/html/body/div[1]/div[2]/div[1]/div[2]/div/div/div[6]/section/div/div[1]/div/div[2]/div[2]/button[3]')
                futures_button[0].click()
                table = driver.find_element(By.TAG_NAME, 'tbody')
                rows = table.find_elements(By.TAG_NAME,'tr')
                all_data = []
                for i in range(10):
                    temp = []
                    counter=-1
                    for paraph in rows[i].find_elements(By.TAG_NAME,'p') and rows[i].find_elements(By.TAG_NAME,'td'):
                        counter+=1
                        if counter in [3,4,5,6,7,9]:
                            value = paraph.text.replace("$", "").replace(",", "").replace("*", "").replace("%", "").replace("--", "0").replace("<0.01", "0.001")
                            if "<0.01" in paraph.text :
                                value = 0.001
                            try:
                                float_value = float(value)
                            except:
                                float_value = 0.001
                                
                            temp.append(float_value)
                        else:
                            temp.append(paraph.text)

                    if "USD" in temp[2]:
                        all_data.append(temp)

                all_data = pd.DataFrame(all_data)
                db.connection.insertTOfutures(timestamp, np.mean(all_data[3]), np.mean(all_data[4]),
                                                np.mean(all_data[5]),np.sum(all_data[6]),np.sum(all_data[7]),
                                                np.sum(all_data[9]))
            else:
                break

        # print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

job()
flag =0
if flag ==0:
      flag=1
      db.connection.create_database()
      db.connection.create_tables(0)
      db.connection.create_tables(1)
      db.connection.create_tables(2)


schedule.every(1).minutes.do(job)
end_time = time.time() + 60 * 9999999999
while time.time() < end_time:
    schedule.run_pending()
    time.sleep(5)

