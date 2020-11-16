import sqlite3
import pandas as pd 

class Dqlab_store_3:
    def __init__(self, database):
        self.database = database

    def open_connect(self):
        self.connection = sqlite3.connect(self.database)
        return print('database connected')

    def trc_12476(self):
        # menampilkan 10 transaksi pembelian dari pengguna dengan user_id 12476
        query = 'SELECT seller_id, \
                        buyer_id, \
                        total as nilai_transaksi, \
                        created_at as tanggal_transaksi \
                FROM orders \
                WHERE buyer_id = 12476 \
                ORDER BY 3 DESC \
                LIMIT 10;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        data_12476 = pd.DataFrame(cursor.fetchall(), columns=['seller_id', 'buyer_id', 'nilai_transaksi', 'tanggal_transaksi'])
        cursor.close()
        print('\n10 transaksi pembelian dari pengguna dengan user_id 12476:')
        print(data_12476)

    def trans_per_month(self):
        query = "SELECT strftime('%Y%m', created_at) as tahun_bulan, \
                        COUNT(1) as jumlah_transaksi, \
                        SUM(total) as total_nilai_transaksi \
                FROM orders \
                WHERE created_at >= '2020-01-01' \
                GROUP BY 1 \
                ORDER BY 1;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_per_Month = pd.DataFrame(cursor.fetchall(), columns=['tahun_bulan', 'jumlah_transaksi', 'total_nilai_transaksi'])
        cursor.close()
        print('\nSummary transaksi per bulan di tahun 2020:')
        print(trans_per_Month)

    def largest_avg_transaction(self):
        query = "SELECT buyer_id, \
                        count(1) as jumlah_transaksi, \
                        avg(total) as avg_nilai_transaksi \
                FROM orders \
                WHERE created_at >= '2020-01-01' AND created_at < '2020-02-01' \
                GROUP BY 1 \
                HAVING COUNT(1) >= 2 \
                ORDER BY 3 DESC \
                LIMIT 10;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        largest_avg_trans = pd.DataFrame(cursor.fetchall(), columns=['tahun_bulan', 'jumlah_transaksi', 'total_nilai_transaksi'])
        cursor.close()
        print('\n10 pembeli dengan rata-rata nilai transaksi terbesar yang bertransaksi minimal 2 kali di Januari 2020:')
        print(largest_avg_trans)

    def big_trans(self):
        query = "SELECT nama_user as nama_pembeli, \
                        total as nilai_transaksi, \
                        created_at as tanggal_transaksi \
                FROM orders \
                INNER JOIN users on buyer_id = user_id \
                WHERE created_at >= '2019-12-01' AND created_at < '2020-01-01' AND total >= 20000000 \
                ORDER BY 1;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        big_transDes2019 = pd.DataFrame(cursor.fetchall(), columns=['tahun_bulan', 'jumlah_transaksi', 'total_nilai_transaksi'])
        cursor.close()
        print('\nTransaksi minimal 20,000,000 di bulan Desember 2019:')
        print(big_transDes2019)

    def best_selling_product(self):
        query = "SELECT category, \
                        SUM(quantity) as total_quantity, \
                        SUM(price) as total_price \
                FROM orders \
                INNER JOIN order_details using(order_id) \
                INNER JOIN products using(product_id) \
                WHERE created_at >= '2020-01-01' AND delivery_at IS NOT NULL \
                GROUP BY 1 \
                ORDER BY 2 DESC \
                LIMIT 5;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        best_selling_product2020 = pd.DataFrame(cursor.fetchall(), columns=['tahun_bulan', 'jumlah_transaksi', 'total_nilai_transaksi'])
        cursor.close()
        print('\n5 Kategori dengan total quantity terbanyak di tahun 2020:')
        print(best_selling_product2020)

    def close_connect(self):
        self.connection.commit()
        self.connection.close()
        print('connection close')

database = 'Data Analysis for E-Commerce Challenge/data/dqlab_store.db'

app = Dqlab_store_3(database)
app.open_connect()
app.trc_12476()
app.trans_per_month()
app.largest_avg_transaction()
app.big_trans()
app.best_selling_product()
app.close_connect()