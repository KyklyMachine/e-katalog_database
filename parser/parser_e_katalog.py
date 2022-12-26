import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import random
import names


BASE_URL = "https://kz.e-katalog.com/"
URL_SMARTPHONES = "https://kz.e-katalog.com/list/122/"
URL_LAPTOPS = "https://kz.e-katalog.com/list/239/"
PAGES_COUNT = 10
USERS_COUNT = 100


class Parser:
    items = pd.DataFrame(columns=["name",
                                  "scores",
                                  "characteristic1",
                                  "characteristic2",
                                  "characteristic3",
                                  "characteristic4",
                                  "characteristic5",
                                  "characteristic6",
                                  "characteristic7",
                                  "mods"])

    stores = pd.DataFrame(columns=["name",
                                   "homepage"])

    users = pd.DataFrame(columns=["nickname",
                                  "email",
                                  "password"])

    def parse_items(self, input_url):
        for page in range(PAGES_COUNT):
            url = str(input_url + f"{page}/".format(page))
            r = requests.get(url)
            soup = bs(r.text, "html.parser")
            card = soup.find_all(class_="model-short-block")

            for item in card:
                short_info = item.find(class_="model-short-info")

                # name
                name = short_info.find(class_="model-short-title no-u").get("title").replace("\xa0", "")

                # scores
                scores_list = short_info.find(class_="short-opinion-icons")
                scores = ""
                if len(scores_list) == 4:
                    i = 0
                    for element in scores_list:
                        element_title = element.get("title")
                        if element_title:
                            scores += str(element_title.split(" ")[-1][1])
                        else:
                            scores += "0"
                        if i != 3:
                            scores += ";"

                        i += 1
                if scores == "":
                    scores = "0;0;0;0"

                # characteristics 1 to 7
                characteristics = short_info.find(class_="m-s-f2").find_all("div")
                characteristic1 = characteristics[0].get("title").replace("\xa0", "")
                characteristic2 = characteristics[2].get("title").replace("\xa0", "")
                characteristic3 = characteristics[4].get("title").replace("\xa0", "")
                if len(characteristics) < 7:
                    characteristic4 = ""
                else:
                    characteristic4 = characteristics[6].get("title").replace("\xa0", "")
                if len(characteristics) < 9:
                    characteristic5 = ""
                else:
                    characteristic5 = characteristics[8].get("title").replace("\xa0", "")
                if len(characteristics) < 11:
                    characteristic6 = ""
                else:
                    characteristic6 = characteristics[10].get("title").replace("\xa0", "")
                if len(characteristics) < 13:
                    characteristic7 = characteristic6
                    characteristic6 = ""
                else:
                    characteristic7 = characteristics[12].get("title").replace("\xa0", "")

                # mods
                mods_list = short_info.find(class_="m-c-f1-pl--button")
                mods = name
                if mods_list:
                    mods = ""
                    mods_list = mods_list.find_all("span")
                    for element in mods_list:
                        if mods_list[len(mods_list) - 1] == element:
                            mods += str(element.get("title").replace("\xa0", ""))
                        else:
                            mods += str(element.get("title").replace("\xa0", "")) + ";"

                # prices

                prices = item.find(class_="l-pr-pd")
                if prices:
                    all_price = prices.find_all("span")
                    lowest_price = 0
                    hightest_price = 0
                    for p in all_price[0]:
                        lowest_price = int(p.replace("\xa0", "").replace("тг.", ""))
                    if len(all_price) != 1:
                        for p in all_price[1]:
                            hightest_price = int(p.replace("\xa0", "").replace("тг.", ""))
                    else:
                        hightest_price = lowest_price + random.randint(0, 200000)

                    prices = str(lowest_price) + ";" + str(hightest_price)

                else:
                    lowest_price = random.randint(50000, 400000)
                    prices = str(lowest_price) + ";" + str(lowest_price + random.randint(0, 200000))

                # insertion in dataframe
                self.items.loc[len(self.items)] = [name,
                                                   scores,
                                                   characteristic1,
                                                   characteristic2,
                                                   characteristic3,
                                                   characteristic4,
                                                   characteristic5,
                                                   characteristic6,
                                                   characteristic7,
                                                   mods]
    def parse_stores(self):
        """
        Да-да, это парсинг)))
        """
        stores = {
            "МВидео": "https://www.mvideo.ru/",
            "Ситилинк": "https://www.citilink.ru/",
            "ДНС": "https://www.dns-shop.ru/",
            "Эльдорадо": "https://www.eldorado.ru/",
            "BigGeek": "https://biggeek.ru/",
            "Связной": "https://www.svyaznoy.ru/",
            "Restore": "https://re-store.ru/",
            "Holodilnik": "https://www.holodilnik.ru/",
            "СберМегаМаркет": "https://sbermegamarket.ru/",
            "OLDI": "https://www.oldi.ru/",
            "Корпорация «Центр»": "https://kcentr.ru/",
            "Юлмарт": "https://ulmart-katalog.ru/",
            "Технопорт": "https://www.techport.ru/"}
        for item in stores:
            self.stores.loc[len(self.stores)] = [item, stores[item]]

    def parse_users(self):
        for i in range(USERS_COUNT):
            nickname = str(names.get_first_name()) + str(random.randint(10, 100))
            email = nickname + "@gmail.com"
            password = nickname + str(random.randint(1000, 9999))
            # print(nickname)
            data = [nickname, email, password]
            self.users.loc[len(self.users)] = data

if __name__ == "__main__":
    parser = Parser()
    parser.parse_items(URL_LAPTOPS)
    parser.items.to_csv("items.csv")
    #parser.parse_stores()
    #parser.stores.to_csv("stores.csv")
    #parser.parse_users()
    #parser.users.to_csv("users.csv")