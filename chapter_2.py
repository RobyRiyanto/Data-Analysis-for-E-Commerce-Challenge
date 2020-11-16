import sqlite3
import pandas as pd

class Dqlab_store_data:
    def __init__(self, database):
        self.database = database

    def open_connect(self):
        self.connection = sqlite3.connect(self.database)
        return print('database connected')

    def show_products(self):
        query = 'SELECT * FROM products;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        data_produk = pd.DataFrame(cursor.fetchall())
        cursor.close()
        print(data_produk.head())

        query = 'SELECT COUNT(*) FROM products;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_data_produk = cursor.fetchall()
        cursor.close()
        print('Jumlah Data Produk =', jml_data_produk)

        query = 'SELECT category, COUNT(*) as jumlah FROM products GROUP BY category;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_kategori_produk = pd.DataFrame(cursor.fetchall(), columns=['category', 'jumlah'])
        cursor.close()
        print(jml_kategori_produk)

    def show_orders(self):
        query = 'SELECT * FROM orders;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        data_order = pd.DataFrame(cursor.fetchall())
        cursor.close()
        print(data_order.head())

        query = 'SELECT COUNT(*) FROM orders;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_data_order = cursor.fetchall()
        cursor.close()
        print('Jumlah Data Order =', jml_data_order)

    def trans_bulanan(self):
        query = 'SELECT COUNT(*) FROM orders WHERE created_at between "2019-09-01" and "2019-09-30";'
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_sept2019 = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Bulan September 2019 =', trans_sept2019)

        query = 'SELECT COUNT(*) FROM orders WHERE created_at between "2019-10-01" and "2019-10-31";'
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_nov2019 = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Bulan November 2019 =', trans_nov2019)

        query = 'SELECT COUNT(*) FROM orders WHERE created_at between "2020-01-01" and "2020-01-31";'
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_jan2020 = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Bulan Januari 2020 =', trans_jan2020)

        query = 'SELECT COUNT(*) FROM orders WHERE created_at between "2020-03-01" and "2020-03-31";'
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_mar2020 = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Bulan Januari 2020 =', trans_mar2020)

        query = 'SELECT COUNT(*) FROM orders WHERE created_at between "2020-05-01" and "2020-05-31";'
        cursor = self.connection.cursor()
        cursor.execute(query)
        trans_mei2020 = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Bulan Januari 2020 =', trans_mei2020)

    def status_transaksi(self):
        query = "SELECT COUNT(*) FROM orders WHERE paid_at = 'NA';"
        cursor = self.connection.cursor()
        cursor.execute(query)
        not_paid = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Yang Tidak Dibayar =', not_paid)

        query = "SELECT COUNT(*) FROM orders WHERE delivery_at IS 'NA' AND paid_at IS NOT 'NA';"
        cursor = self.connection.cursor()
        cursor.execute(query)
        paid_not_delivered = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Yang Sudah Dibayar Tapi Tidak Dikirim, Baik Yang Sudah Dibayar Maupun Belum =', paid_not_delivered)

        query = "SELECT COUNT(*) FROM orders WHERE delivery_at IS 'NA';"
        cursor = self.connection.cursor()
        cursor.execute(query)
        not_delivered = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Yang Tidak Dikirim =', not_delivered)

        query = "SELECT COUNT(*) FROM orders WHERE paid_at = delivery_at AND paid_at IS NOT 'NA';"
        cursor = self.connection.cursor()
        cursor.execute(query)
        not_delivered = cursor.fetchall()
        cursor.close()
        print('Jumlah Transaksi Yang Dikirim Pada Hari Yang Sama Dengan Tanggal Bayar =', not_delivered)

    def show_users(self):
        query = "SELECT COUNT(*) FROM users;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_user_trans = cursor.fetchall()
        cursor.close()
        print('Jumlah User =', jml_user_trans)

        query = 'SELECT buyer_id, COUNT(*) as jumlah FROM orders GROUP BY buyer_id;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_user_as_buyer = pd.DataFrame(cursor.fetchall(), columns=['buyer_id', 'jumlah'])
        cursor.close()
        print('Jumlah User Yang Pernah Bertransaksi Sebagai Pembeli =', len(jml_user_as_buyer))

        query = 'SELECT seller_id, COUNT(*) as jumlah FROM orders GROUP BY seller_id;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        jml_user_as_seller = pd.DataFrame(cursor.fetchall(), columns=['seller_id', 'jumlah'])
        cursor.close()
        print('Jumlah User Yang Pernah Bertransaksi Sebagai Penjual =', len(jml_user_as_seller))

    def top_buyer(self):
        query = 'SELECT buyer_id, COUNT(*) as jumlah_order, total as total_nilai_order FROM orders GROUP BY buyer_id ORDER BY total_nilai_order DESC;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        top_buyer = pd.DataFrame(cursor.fetchall(), columns=['buyer_id', 'jumlah_order', 'total_nilai_order'])
        cursor.close()
        print(top_buyer.head(10))

        query = "SELECT buyer_id, total as total_nilai_order FROM orders WHERE buyer_id IN ('14411', '10977', '1251', '15915', '10355') ORDER BY 2 DESC;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        top_buyer_most_purchases = pd.DataFrame(cursor.fetchall(), columns=['buyer_id', 'total_order'])
        cursor.close()
        print('\n5 pembeli dengan dengan total pembelian terbesar (berdasarkan total harga barang setelah diskon):')
        print(top_buyer_most_purchases.head())

        query = "SELECT buyer_id, COUNT(*) as jumlah_order, subtotal, discount, total FROM orders WHERE discount = 0 GROUP BY 1 ORDER BY 2 DESC;"
        cursor = self.connection.cursor()
        cursor.execute(query)
        top_user_buyer = pd.DataFrame(cursor.fetchall(), columns=['buyer_id', 'jumlah_order', 'subtotal', 'discount', 'total'])
        cursor.close()
        print('\nPengguna yang tidak pernah menggunakan diskon ketika membeli barang dan merupakan 5 pembeli dengan transaksi terbanyak:')
        print(top_user_buyer.head(20))

    def top_product(self):
        query = 'SELECT a.created_at, \
                        b.order_id, \
                        b.product_id, \
                        c.desc_product, \
                        b.quantity \
                FROM orders a \
                INNER JOIN order_details b ON a.order_id = b.order_id \
                INNER JOIN products c ON b.product_id = c.product_id \
                WHERE a.created_at between "2019-12-01" and "2019-12-31" \
                GROUP BY 3 \
                ORDER BY 5 DESC;'
        cursor = self.connection.cursor()
        cursor.execute(query)
        top_productDes2019 = pd.DataFrame(cursor.fetchall(), columns=['buyer_id', 'jumlah_order', 'subtotal', 'discount', 'total'])
        cursor.close()
        print('\nPengguna yang bertransaksi setidaknya 1 kali setiap bulan di tahun 2020 dengan rata-rata total amount per transaksi lebih dari 1 Juta: ')
        print(top_productDes2019)

    def close_connect(self):
        self.connection.commit()
        self.connection.close()
        print('connection close')

database = 'Data Analysis for E-Commerce Challenge/data/dqlab_store.db'

app = Dqlab_store_data(database)
app.open_connect()
app.show_products()
app.show_orders()
app.trans_bulanan()
app.status_transaksi()
app.show_users()
app.top_buyer()
app.top_product()
app.close_connect()