import sqlite3
import pandas as pd 

class Dqlab_store_4:
    def __init__(self, database):
        self.database = database

    def open_connect(self):
        self.connection = sqlite3.connect(self.database)
        return print('database connected')

    def high_value_buyer(self):
        query = 'SELECT nama_user as nama_pembeli, \
                        COUNT(1) as jumlah_transaksi, \
                        SUM(total) as total_nilai_transaksi, \
                        MIN(total) as min_nilai_transaksi \
                FROM orders \
                INNER JOIN users ON buyer_id = user_id \
                GROUP BY user_id, nama_user \
                HAVING COUNT(1) > 5 AND MIN(total) > 2000000 \
                ORDER BY 3 DESC;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        highValue_buyer = pd.DataFrame(cursor.fetchall(), columns=['nama_pembeli', 'jumlah_transaksi', 'total_nilai_transaksi', 'min_nilai_transaksi'])
        cursor.close()
        print('\nPembeli yang sudah bertransaksi lebih dari 5 kali, dan setiap transaksi lebih dari 2,000,000:')
        print(highValue_buyer)

    def findDropshipper(self):
        query = "SELECT nama_user as nama_pembeli, \
                        COUNT(1) as jumlah_transaksi, \
                        COUNT(DISTINCT orders.kodepos) as distinct_kodepos, \
                        SUM(total) as total_nilai_transaksi, \
                        AVG(total) as avg_nilai_transaksi \
                FROM orders \
                INNER JOIN users ON buyer_id = user_id \
                GROUP BY user_id, nama_user \
                HAVING COUNT(1) >= 10 AND COUNT(1) = COUNT(DISTINCT orders.kodepos) \
                ORDER BY 2 DESC;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        findUser_asDropshipper = pd.DataFrame(cursor.fetchall(), columns=['nama_pembeli', 'jumlah_transaksi', 'distinct_kodepos', 'total_nilai_transaksi', 'avg_nilai_transaksi'])
        cursor.close()
        print('\nPengguna yang menjadi dropshipper:')
        print(findUser_asDropshipper)

    def findReseller(self):
        query = "SELECT nama_user as nama_pembeli, \
                        COUNT(1) as jumlah_transaksi, \
                        SUM(total) as total_nilai_transaksi, \
                        AVG(total) as avg_nilai_transaksi, \
                        AVG(total_quantity) as avg_quantity_per_transaksi \
                FROM orders \
                INNER JOIN users on buyer_id = user_id \
                INNER JOIN (select order_id, SUM(quantity) as total_quantity FROM order_details group by 1) as summary_order using(order_id) \
                WHERE orders.kodepos = users.kodepos \
                GROUP BY user_id, nama_user \
                HAVING COUNT(1) >= 8 AND AVG(total_quantity) > 10 \
                ORDER BY 3 DESC;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        largest_avg_trans = pd.DataFrame(cursor.fetchall(), columns=['nama_pembeli', 'jumlah_transaksi', 'total_nilai_transaksi', 'avg_nilai_transaksi', 'avg_quantity_per_transaksi'])
        cursor.close()
        print('\n10 pembeli dengan rata-rata nilai transaksi terbesar yang bertransaksi minimal 2 kali di Januari 2020:')
        print(largest_avg_trans)

    def sellerAnd_buyer(self):
        query = "SELECT nama_user as nama_pengguna, \
                        jumlah_transaksi_beli, \
                        jumlah_transaksi_jual \
                FROM users \
                INNER JOIN (select buyer_id, count(1) as jumlah_transaksi_beli from orders group by 1) as buyer on buyer_id = user_id \
                INNER JOIN (select seller_id, count(1) as jumlah_transaksi_jual from orders group by 1) as seller on seller_id = user_id \
                WHERE jumlah_transaksi_beli>=7 \
                ORDER BY 1;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        seller_and_buyer = pd.DataFrame(cursor.fetchall(), columns=['nama_pengguna', 'jumlah_transaksi_beli', 'jumlah_transaksi_jual'])
        cursor.close()
        print('\nPenjual yang juga pernah bertransaksi sebagai pembeli minimal 7 kali:')
        print(seller_and_buyer)

    def lamaTrans_dibayar(self):
        query = "SELECT strftime('%Y%m', created_at) as tahun_bulan, \
                        COUNT(1) as jumlah_transaksi, \
                        AVG(JulianDay(paid_at) - JulianDay(created_at)) as avg_lama_dibayar, \
                        MIN(JulianDay(paid_at) - JulianDay(created_at)) min_lama_dibayar, \
                        MAX(JulianDay(paid_at) - JulianDay(created_at)) max_lama_dibayar \
                FROM orders \
                WHERE paid_at IS NOT NULL \
                GROUP BY 1 \
                ORDER BY 1;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        time_paid = pd.DataFrame(cursor.fetchall(), columns=['tahun_bulan', 'jumlah_transaksi', 'avg_lama_dibayar', 'min_lama_dibayar', 'max_lama_dibayar'])
        cursor.close()
        print('\nTrend lama waktu transaksi dibayar sejak dibuat:')
        print(time_paid)

    def close_connect(self):
        self.connection.commit()
        self.connection.close()
        print('connection close')

database = 'Data Analysis for E-Commerce Challenge/data/dqlab_store.db'

app = Dqlab_store_4(database)
app.open_connect()
app.high_value_buyer()
app.findDropshipper()
app.findReseller()
app.sellerAnd_buyer()
app.lamaTrans_dibayar()
app.close_connect()