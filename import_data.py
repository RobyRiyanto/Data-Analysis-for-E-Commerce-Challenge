import sqlite3
import csv

class Dqlab_store:
    def __init__(self, database):
        self.database = database

    def open_connect(self):
        self.connection = sqlite3.connect(self.database)
        return print('database connected')

    def import_data(self, users, product, orders, order_details):
        # Create Table products and Import Data
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS products (product_id INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL, desc_product TEXT NOT NULL, category TEXT NOT NULL, base_price INTEGER NOT NULL);") 

        with open(product,'r') as data_products:
            dr = csv.DictReader(data_products, delimiter=';') # comma is default delimiter
            to_db = [(i['product_id'], i['desc_product'], i['category'], i['base_price']) for i in dr]

        cursor.executemany("INSERT INTO products (product_id, desc_product, category, base_price) VALUES (?, ?, ?, ?);", to_db)
        cursor.close()

        # Create Table users and Import Data
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL, nama_user TEXT NOT NULL, kodepos TEXT NOT NULL, email TEXT NOT NULL);") 

        with open(users,'r') as data_users:
            dr = csv.DictReader(data_users, delimiter=';') # comma is default delimiter
            to_db = [(i['user_id'], i['nama_user'], i['kodepos'], i['email']) for i in dr]

        cursor.executemany("INSERT INTO users (user_id, nama_user, kodepos, email) VALUES (?, ?, ?, ?);", to_db)
        cursor.close()

        # Create Table orders and Import Data
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS orders (order_id INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL, seller_id INTEGER NOT NULL, buyer_id INTEGER NOT NULL, kodepos TEXT NOT NULL, subtotal INTEGER NOT NULL, discount INTEGER NOT NULL, total INTEGER NOT NULL, created_at DATETIME NOT NULL, paid_at DATETIME NOT NULL, delivery_at DATETIME NOT NULL);") 

        with open(orders,'r') as data_orders:
            dr = csv.DictReader(data_orders, delimiter=';') # comma is default delimiter
            to_db = [(i['order_id'], i['seller_id'], i['buyer_id'], i['kodepos'], i['subtotal'], i['discount'], i['total'], i['created_at'], i['paid_at'], i['delivery_at']) for i in dr]

        cursor.executemany("INSERT INTO orders (order_id, seller_id, buyer_id, kodepos, subtotal, discount, total, created_at, paid_at, delivery_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
        cursor.close()

        # Create Table order_details and Import Data
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS order_details (order_detail_id INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL, order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, price INTEGER NOT NULL, quantity INTEGER NOT NULL);") 

        with open(order_details,'r') as data_order_details:
            dr = csv.DictReader(data_order_details, delimiter=';') # comma is default delimiter
            to_db = [(i['order_detail_id'], i['order_id'], i['product_id'], i['price'], i['quantity']) for i in dr]

        cursor.executemany("INSERT INTO order_details (order_detail_id, order_id, product_id, price, quantity) VALUES (?, ?, ?, ?, ?);", to_db)
        cursor.close()

    def close_connect(self):
        self.connection.commit()
        self.connection.close()
        print('connection close')

database = 'Data Analysis for E-Commerce Challenge/data/dqlab_store.db'
orders = 'Data Analysis for E-Commerce Challenge/data/orders.csv'
order_details = 'Data Analysis for E-Commerce Challenge/data/order_details.csv'
product = 'Data Analysis for E-Commerce Challenge/data/products.csv'
users = 'Data Analysis for E-Commerce Challenge/data/users.csv'

app = Dqlab_store(database)
app.open_connect()
app.import_data(users, product, orders, order_details)
app.close_connect()