import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

connection_string = "DRIVER={SQL Server};SERVER=DESKTOP-QT7GSU5;DATABASE=Rekrutacja;Trusted_Connection=yes"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

engine = create_engine(connection_url)
engine2 = create_engine("postgresql://postgres:Lijopleurodon12#4@localhost:5432/Rekrutacja")

conn = engine.connect()
conn2 = engine2.connect()

customer_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Customer", engine)
order_df = pd.read_sql_query("select * from [Rekrutacja].dbo.[Order]", engine)
orderitem_df = pd.read_sql_query("select * from [Rekrutacja].dbo.OrderItem", engine)
product_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Product", engine)
supplier_df = pd.read_sql_query("select * from [Rekrutacja].dbo.Supplier", engine)

product_df["IsDiscontinued"] = product_df["IsDiscontinued"]*1

customer_df_pst = pd.read_sql_query('SELECT * FROM public."Customer2"', engine2)
order_df_pst = pd.read_sql_query('SELECT * FROM public."Order2"', engine2)
orderitem_df_pst = pd.read_sql_query('SELECT * FROM public."OrderItem2"', engine2)
product_df_pst = pd.read_sql_query('SELECT * FROM public."Product2"', engine2)
supplier_df_pst = pd.read_sql_query('SELECT * FROM public."Supplier2"', engine2)

missing_c = pd.concat([customer_df, customer_df_pst]).drop_duplicates(keep=False)
missing_o = pd.concat([order_df, order_df_pst]).drop_duplicates(keep=False)
missing_oi = pd.concat([orderitem_df, orderitem_df_pst]).drop_duplicates(keep=False)
missing_p = pd.concat([product_df, product_df_pst]).drop_duplicates(keep=False)
missing_s = pd.concat([supplier_df, supplier_df_pst]).drop_duplicates(keep=False)

missing_c.to_sql("Customer2", engine2, index=False, if_exists="append")
missing_o.to_sql("Customer2", engine2, index=False, if_exists="append")
missing_oi.to_sql("Customer2", engine2, index=False, if_exists="append")
missing_p.to_sql("Customer2", engine2, index=False, if_exists="append")
missing_s.to_sql("Customer2", engine2, index=False, if_exists="append")

conn.close()
conn2.close()