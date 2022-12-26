import random

import pandas as pd
import psycopg2
from psycopg2 import Error
import constants
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import parser_e_katalog


class Database:
    connection = None
    cursor = None
    db_name = ""
    db_username = ""
    db_password = ""
    db_host = ""
    db_port = ""

    def __init__(self, db_name, db_username, db_password, db_host, db_port):
        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def open_connection(self):
        try:
            self.connection = psycopg2.connect(dbname=self.db_name,
                                               user=self.db_username,
                                               password=self.db_password,
                                               host=self.db_host,
                                               port=self.db_port)
            self.cursor = self.connection.cursor()
            print("Открыто соединение с PostgreSQL")

        except(Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Соединение с PostgreSQL закрыто")

    def write_users(self):
        try:
            users = pd.read_csv("users.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"Users\" (" \
                             "id," \
                             "nickname," \
                             "email," \
                             "pass" \
                             ") VALUES (%s,%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), user[1], user[2], user[3]))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_stores(self):
        try:
            stores = pd.read_csv("stores.csv")
            for i in range(stores.shape[0]):
                store = stores.iloc[i]
                sql_insert = "INSERT INTO \"Shop\" (" \
                             "id," \
                             "shop_name," \
                             "rating," \
                             "review_count," \
                             "home_page" \
                             ") VALUES (%s,%s,%s,%s,%s);"
                self.cursor.execute(sql_insert, (str(i), store[1], int(0), int(0), store[2]))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_brands(self):
        try:
            # brands that already exist
            sql_request = "SELECT id, brand_name FROM \"Brands\""
            self.cursor.execute(sql_request)
            brands = self.cursor.fetchall()  # [(0, 'Apple'), (1, 'Xiaomi'), ...]
            exist_brands = []
            for brand in brands:
                exist_brands.append(brand[1])

            items = pd.read_csv("items.csv").loc[:, "name"]
            ls = []
            for i in range(len(items)):
                items.iloc[i] = items.iloc[i].split(" ")[1:2]

                if items.iloc[i][0] not in ls and items.iloc[i][0] not in exist_brands:
                    ls.append(items.iloc[i][0])
            for j in range(len(ls)):
                sql_insert = "INSERT INTO \"Brands\" (" \
                             "id," \
                             "brand_name," \
                             "brand_info" \
                             ") VALUES (%s,%s,%s);"
                self.cursor.execute(sql_insert, (str(j+len(exist_brands)), ls[j], str("Just a " + ls[j])))
                self.connection.commit()

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_products(self, category_id):
        try:
            sql_request = "SELECT id, brand_name FROM \"Brands\""
            self.cursor.execute(sql_request)
            brands = self.cursor.fetchall()  # [(0, 'Apple'), (1, 'Xiaomi'), ...]
            brands_names = []
            for brand in brands:
                brands_names.append(brand[1])

            # Getting all categories with it's id
            sql_request = "SELECT COUNT(*) FROM \"Product\""
            self.cursor.execute(sql_request)
            count = self.cursor.fetchall()[0][0]
            print(count)

            # Reading items
            items = pd.read_csv("items.csv").loc[:, "name"]

            for i in range(len(items)):
                sql_insert = "INSERT INTO \"Product\" (" \
                             "id," \
                             "brand_id," \
                             "category_id," \
                             "product_name" \
                             ") VALUES (%s,%s,%s,%s);"
                item_brand = items.iloc[i].split(" ")[1]
                index = brands_names.index(item_brand)
                self.cursor.execute(sql_insert, (str(i+count), brands[index][0], category_id, items.iloc[i]))
                self.connection.commit()

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_products_mod(self):
        try:
            # Getting all products
            sql_request = "SELECT id, product_name FROM \"Product\""
            self.cursor.execute(sql_request)
            sql_products = self.cursor.fetchall()
            from_sql_products = pd.DataFrame(columns=["id", "name"])
            for i in range(len(sql_products)):
                from_sql_products.loc[len(from_sql_products)] = [sql_products[i][0], sql_products[i][1]]
            # print(from_sql_products)

            # Getting parsed dataframe
            df_product = (pd.read_csv("items.csv"))[["name", "mods"]]
            # print(df_product)
            index = 0

            sql_request = "SELECT COUNT(*) FROM \"ProductMod\""
            self.cursor.execute(sql_request)
            count = self.cursor.fetchall()[0][0]
            print(count)

            for i in range(len(from_sql_products)):

                mods = df_product.iloc[i][1].split(";")
                # print(len(mods))

                for j in range(len(mods)):
                    sql_insert = "INSERT INTO \"ProductMod\" (" \
                                 "id," \
                                 "product_id," \
                                 "product_mod_name" \
                                 ") VALUES (%s,%s,%s);"
                    # print([str(i+j), from_sql_products.iloc[i][0], mods[j]])
                    self.cursor.execute(sql_insert, (str(index + count), from_sql_products.iloc[i][0], mods[j]))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_shop_reviews(self):
        try:
            # Getting all users
            sql_request = "SELECT id FROM \"Users\""
            self.cursor.execute(sql_request)
            sql_user = self.cursor.fetchall()
            from_sql_user = pd.DataFrame(columns=["id"])
            for i in range(len(sql_user)):
                from_sql_user.loc[len(from_sql_user)] = [sql_user[i][0]]
            # print(from_sql_user)

            # Getting all shops
            sql_request = "SELECT id FROM \"Shop\""
            self.cursor.execute(sql_request)
            sql_shop = self.cursor.fetchall()
            from_sql_shop = pd.DataFrame(columns=["id"])
            for i in range(len(sql_shop)):
                from_sql_shop.loc[len(from_sql_shop)] = [sql_shop[i][0]]
            # print(from_sql_shop)

            index = 0
            for i in range(len(from_sql_shop)):
                for j in range(len(from_sql_user)):
                    sql_insert = "INSERT INTO \"ShopReview\" (" \
                                 "id," \
                                 "customer_id," \
                                 "shop_id,"\
                                 "review," \
                                 "score" \
                                 ") VALUES (%s,%s,%s,%s,%s);"
                    score = random.randint(1, 5)
                    review = ""
                    if score == 1 or score == 2:
                        review = constants.BAD_STORE_REVIEWS[random.randint(0, len(constants.BAD_STORE_REVIEWS) - 1)]
                    if score == 3 or score == 4:
                        review = constants.NORMAL_STORE_REVIEWS[random.randint(0, len(constants.NORMAL_STORE_REVIEWS) - 1)]
                    if score == 5:
                        review = constants.GOOD_STORE_REVIEWS[random.randint(0, len(constants.GOOD_STORE_REVIEWS) - 1)]
                    self.cursor.execute(sql_insert, (str(index),
                                                     from_sql_user.iloc[random.randint(0, len(from_sql_user) - 1)][0],
                                                     from_sql_shop.iloc[i][0],
                                                     review,
                                                     score))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_search_story(self):
        try:
            # Getting all user
            sql_request = "SELECT id FROM \"Users\""
            self.cursor.execute(sql_request)
            sql_user = self.cursor.fetchall()
            from_sql_user = pd.DataFrame(columns=["id"])
            for i in range(len(sql_user)):
                from_sql_user.loc[len(from_sql_user)] = [sql_user[i][0]]
            # print(from_sql_user)

            # Getting all productMods
            sql_request = "SELECT id FROM \"ProductMod\""
            self.cursor.execute(sql_request)
            sql_product_mod = self.cursor.fetchall()
            from_sql_product_mod = pd.DataFrame(columns=["id"])
            for i in range(len(sql_product_mod)):
                from_sql_product_mod.loc[len(from_sql_product_mod)] = [sql_product_mod[i][0]]
            # print(from_sql_shop)

            index = 0
            for i in range(len(from_sql_product_mod)):
                for j in range(len(from_sql_user)):
                    sql_insert = "INSERT INTO \"SearchStory\" (" \
                                 "id," \
                                 "customer_id," \
                                 "model_id," \
                                 "search_date" \
                                 ") VALUES (%s,%s,%s,%s);"
                    date = str(random.randint(2020, 2022)) + "-" + str(random.randint(1, 12)) + "-" + str(random.randint(1, 28))

                    self.cursor.execute(sql_insert, (str(index),
                                                     from_sql_user.iloc[random.randint(0, len(from_sql_user) - 1)][0],
                                                     from_sql_product_mod.iloc[i][0],
                                                     date))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_shop_product_mod(self):
        try:
            # Getting all shops
            sql_request = "SELECT id, home_page FROM \"Shop\""
            self.cursor.execute(sql_request)
            sql_shop = self.cursor.fetchall()
            from_sql_shop = pd.DataFrame(columns=["id", "home_page"])
            for i in range(len(sql_shop)):
                from_sql_shop.loc[len(from_sql_shop)] = [sql_shop[i][0], sql_shop[i][1]]

            # Getting all productMods
            sql_request = "SELECT id, product_mod_name FROM \"ProductMod\""
            self.cursor.execute(sql_request)
            sql_product_mod = self.cursor.fetchall()
            from_sql_product_mod = pd.DataFrame(columns=["id", "product_mod_name"])
            for i in range(len(sql_product_mod)):
                from_sql_product_mod.loc[len(from_sql_product_mod)] = [sql_product_mod[i][0], sql_product_mod[i][1]]
            # print(from_sql_shop)

            index = 0
            for i in range(len(from_sql_shop)):
                delta = random.randint(1, len(from_sql_product_mod) - 1)
                start = random.randint(0, len(from_sql_product_mod) - delta)
                stop = start + delta
                for j in range(start, stop):
                    sql_insert = "INSERT INTO \"ShopProductMod\" (" \
                                 "id," \
                                 "product_mod_id," \
                                 "shop_id," \
                                 "cost," \
                                 "link" \
                                 ") VALUES (%s,%s,%s,%s,%s);"

                    cost = random.randint(400, 1400)
                    link = from_sql_shop.iloc[i][1] + "/" + from_sql_product_mod.iloc[j][1].replace(" ", "_").replace("/", "").replace("  ", " ")
                    self.cursor.execute(sql_insert, (str(index),
                                                     from_sql_product_mod.iloc[j][0],
                                                     from_sql_shop.iloc[i][0],
                                                     cost,
                                                     link))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product_reviews(self):
        try:
            # Getting all users
            sql_request = "SELECT id FROM \"Users\""
            self.cursor.execute(sql_request)
            sql_user = self.cursor.fetchall()
            from_sql_user = pd.DataFrame(columns=["id"])
            for i in range(len(sql_user)):
                from_sql_user.loc[len(from_sql_user)] = [sql_user[i][0]]
            # print(from_sql_user)

            # Getting all shops
            sql_request = "SELECT id, product_mod_name FROM \"ProductMod\""
            self.cursor.execute(sql_request)
            sql_product_mod = self.cursor.fetchall()
            from_sql_product_mod = pd.DataFrame(columns=["id", "product_mod_name"])
            for i in range(len(sql_product_mod)):
                from_sql_product_mod.loc[len(from_sql_product_mod)] = [sql_product_mod[i][0], sql_product_mod[i][1]]
            # print(from_sql_shop)

            index = 0
            for i in range(len(from_sql_user)):
                delta = random.randint(2, 10)
                for j in range(len(from_sql_product_mod) // delta):
                    sql_insert = "INSERT INTO \"ProductReview\" (" \
                                 "id," \
                                 "customer_id," \
                                 "product_mod_id,"\
                                 "review," \
                                 "score" \
                                 ") VALUES (%s,%s,%s,%s,%s);"
                    score = random.randint(1, 5)
                    review = ""
                    if score == 1 or score == 2:
                        review = constants.BAD_ITEM_REVIEWS[random.randint(0, len(constants.BAD_ITEM_REVIEWS) - 1)]
                    if score == 3 or score == 4:
                        review = constants.NORMAL_ITEM_REVIEWS[random.randint(0, len(constants.NORMAL_ITEM_REVIEWS) - 1)]
                    if score == 5:
                        review = constants.GOOD_ITEM_REVIEWS[random.randint(0, len(constants.GOOD_ITEM_REVIEWS) - 1)]
                    self.cursor.execute(sql_insert, (str(index),
                                                     from_sql_user.iloc[i][0],
                                                     from_sql_product_mod.iloc[j * delta][0],
                                                     review,
                                                     score))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product_mod_properties(self):
        try:
            mod_properties = ["color", "RAM", "SSD"]

            # Getting all shops
            sql_request = "SELECT id, product_mod_name FROM \"ProductMod\""
            self.cursor.execute(sql_request)
            sql_product_mod = self.cursor.fetchall()
            from_sql_product_mod = pd.DataFrame(columns=["id", "product_mod_name"])
            for i in range(len(sql_product_mod)):
                from_sql_product_mod.loc[len(from_sql_product_mod)] = [sql_product_mod[i][0], sql_product_mod[i][1]]

            index = 0
            for i in range(len(from_sql_product_mod)):
                for j in range(len(mod_properties)):
                    sql_insert = "INSERT INTO \"ProductModProperties\" (" \
                                 "id," \
                                 "product_mod_id," \
                                 "mod_property_id,"\
                                 "product_mod_property" \
                                 ") VALUES (%s,%s,%s,%s);"
                    prop = ""
                    if mod_properties[j] == "color":
                        for k in range(random.randint(1, len(constants.COLORS))):
                            prop += constants.COLORS[k] + "; "
                    elif mod_properties[j] == "RAM":
                        if " 6 " in from_sql_product_mod.iloc[i][1]:
                            prop = "6"
                        elif " 8 " in from_sql_product_mod.iloc[i][1]:
                            prop = "8"
                        elif " 12 " in from_sql_product_mod.iloc[i][1]:
                            prop = "12"
                        else:
                            prop = constants.RAM[random.randint(0, len(constants.RAM) - 1)]
                    elif mod_properties[j] == "SSD":
                        if "32" in from_sql_product_mod.iloc[i][1]:
                            prop = "32"
                        elif "64" in from_sql_product_mod.iloc[i][1]:
                            prop = "64"
                        elif "128" in from_sql_product_mod.iloc[i][1]:
                            prop = "128"
                        elif "256" in from_sql_product_mod.iloc[i][1]:
                            prop = "256"
                        elif "512" in from_sql_product_mod.iloc[i][1]:
                            prop = "512"
                        else:
                            prop = constants.SSD[random.randint(0, len(constants.SSD) - 1)]

                    self.cursor.execute(sql_insert, (str(index),
                                                     from_sql_product_mod.iloc[i][0],
                                                     str(j),
                                                     prop))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product_properties(self):
        try:
            sql_request = "SELECT id FROM \"ProductProperties\""
            self.cursor.execute(sql_request)
            sql_product_mod = self.cursor.fetchall()
            from_sql_product_mod = pd.DataFrame(columns=["id"])
            for i in range(len(sql_product_mod)):
                from_sql_product_mod.loc[len(from_sql_product_mod)] = [sql_product_mod[i][0]]

            properties = ["Назначение", "Конструкция", "Диапазон частот"]

            df_product = (pd.read_csv("items.csv"))[["name", "characteristic1", "characteristic2", "characteristic3",
                                                     "characteristic4", "characteristic5", "characteristic6",
                                                     "characteristic7"]]


            index = 0
            for i in range(len(df_product)):
                for j in range(len(properties)):
                    sql_insert = "INSERT INTO \"ProductProperties\" (" \
                                 "id," \
                                 "product_id," \
                                 "property_id,"\
                                 "value" \
                                 ") VALUES (%s,%s,%s,%s);"
                    prop = ""
                    if properties[j] == "Назначение":
                        prop = df_product.iloc[i][1]
                    if properties[j] == "Конструкция":
                        prop = df_product.iloc[i][2]
                    if properties[j] == "Диапазон частот":
                        prop = df_product.iloc[i][3]
                    if prop == "":
                        continue

                    self.cursor.execute(sql_insert, (str(index + len(from_sql_product_mod)), str(i), str(j), prop))
                    self.connection.commit()
                    index += 1
            index += 1
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()


if __name__ == "__main__":
    db = Database("e_katalog", "postgres", "postgres", "localhost", "5432")
    db.open_connection()
    # db.write_users()
    # db.write_stores()
    #db.write_brands()
    #db.write_products(3)
    # db.write_products_mod()
    # db.write_shop_reviews()
    # db.write_search_story()
    # db.write_shop_product_mod()
    # db.write_product_mod_properties()
    db.write_product_properties()
    db.close_connection()
