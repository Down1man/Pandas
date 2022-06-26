import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.engine import URL

connection_string = "DRIVER={SQL Server};SERVER=DESKTOP-QT7GSU5;DATABASE=Rekrutacja;Trusted_Connection=yes"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

engine = create_engine(connection_url)

customer_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Customer", engine)
order_df = pd.read_sql_query("select * from [Rekrutacja].dbo.[Order]", engine)
orderitem_df = pd.read_sql_query("select * from [Rekrutacja].dbo.OrderItem", engine)
product_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Product", engine)
supplier_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Supplier", engine)

product_df["IsDiscontinued"] = product_df["IsDiscontinued"]*1

print("Customer: \n", customer_df)
print("Order: \n", order_df)
print("OrderItem: \n", orderitem_df)
print("Product: \n", product_df)
print("Supplier: \n", supplier_df)

engine2 = create_engine("postgresql://postgres:Lijopleurodon12#4@localhost:5432/Rekrutacja")

customer_df.to_sql("Customer2", engine2, index=False)
order_df.to_sql("Order2", engine2, index=False)
orderitem_df.to_sql("OrderItem2", engine2, index=False)
product_df.to_sql("Product2", engine2, index=False)
supplier_df.to_sql("Supplier2", engine2, index=False)

'''

conn = psycopg2.connect(
    database="Rekrutacja", user="postgres", password="Lijopleurodon12#4", host="localhost", port='5432'
)

cursor = conn.cursor()

for i in customer_df.index:
    sql = 'INSERT INTO public."Customer"("FirstName", "LastName", "City", "Country", "Phone") VALUES (%s, %s, %s, %s, %s);'
    val = (str(customer_df['FirstName'][i]), str(customer_df['LastName'][i]), str(customer_df['City'][i]), str(customer_df['Country'][i]), str(customer_df['Phone'][i]))
    cursor.execute(sql, val)
    conn.commit()

for i in supplier_df.index:
    sql = 'INSERT INTO public."Supplier"("CompanyName", "ContactName", "ContactTitle", "City", "Country", "Phone", "Fax") VALUES (%s, %s, %s, %s, %s, %s, %s);'
    val = (str(supplier_df['CompanyName'][i]), str(supplier_df['ContactName'][i]), str(supplier_df['ContactTitle'][i]), str(supplier_df['City'][i]), str(supplier_df['Country'][i]), str(supplier_df['Phone'][i]), str(supplier_df['Fax'][i]))
    cursor.execute(sql, val)
    conn.commit()

for i in product_df.index:
    sql = 'INSERT INTO public."Product"("ProductName", "SupplierId", "UnitPrice", "Package", "IsDiscontinued") VALUES (%s, %s, %s, %s, %s);'
    val = (str(product_df['ProductName'][i]), str(product_df['SupplierId'][i]), str(product_df['UnitPrice'][i]), str(product_df['Package'][i]), str(product_df['IsDiscontinued'][i]))
    cursor.execute(sql, val)
    conn.commit()

for i in order_df.index:
    sql = 'INSERT INTO public."Order"("OrderDate", "OrderNumber", "CustomerId", "TotalAmount") VALUES (%s, %s, %s, %s);'
    val = (str(order_df['OrderDate'][i]), str(order_df['OrderNumber'][i]), str(order_df['CustomerId'][i]), str(order_df['TotalAmount'][i]))
    cursor.execute(sql, val)
    conn.commit()

for i in orderitem_df.index:
    sql = 'INSERT INTO public."OrderItem"("OrderId", "ProductId", "UnitPrice", "Quantity") VALUES (%s, %s, %s, %s);'
    val = (str(orderitem_df['OrderId'][i]), str(orderitem_df['ProductId'][i]), str(orderitem_df['UnitPrice'][i]), str(orderitem_df['Quantity'][i]))
    cursor.execute(sql, val)
    conn.commit()

conn.close()
'''