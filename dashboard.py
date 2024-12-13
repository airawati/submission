import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas 
import contextily as ctx
import streamlit as st
from babel.numbers import format_currency
sns.set(style='dark')


def create_daily_orders_df(df):
    daily_orders_df = df.resample(rule='D', on='order_purchase_timestamp').agg({
        "order_id": "nunique",
        "payment_value": "sum"
    })
    daily_orders_df = daily_orders_df.reset_index()
    daily_orders_df.rename(columns={
        "order_id": "order_count",
        "payment_value": "GMV"
    }, inplace=True)
    
    return daily_orders_df

def create_sum_order_items_df(df):
    sum_order_items_df = df.groupby("product_category_name_english").order_id.nunique().sort_values(ascending=False).reset_index()
    return sum_order_items_df

def create_sum_order_reviews_df(df):
    sum_order_reviews_df = df.groupby("product_category_name_english").review_score.mean().sort_values(ascending=False).reset_index()
    sum_order_reviews_df.rename(columns={
    "product_category_name_english": "product_category",
    "review_score": "mean_score_review"
}, inplace=True)
    return sum_order_reviews_df

def create_bystate_df(df):
    bystate_df = df.groupby(by="customer_state").customer_unique_id.nunique().reset_index()
    bystate_df.rename(columns={
        "customer_unique_id": "customer_count"
    }, inplace=True)
    
    return bystate_df

def create_bycity_df(df):
    bycity_df = df.groupby(by="customer_city").customer_unique_id.nunique().reset_index()
    bycity_df.rename(columns={
        "customer_unique_id": "customer_count"
    }, inplace=True)
    
    return bycity_df

def create_customer_maps_df(df):
    customer_maps_df = geopandas.GeoDataFrame(
    df, geometry=geopandas.points_from_xy(df.geolocation_lng, df.geolocation_lat), crs="EPSG:4326"
    )

    return customer_maps_df


def create_rfm_df(df):
    rfm_df = df[df["order_status"] == "delivered"].groupby(by="customer_unique_id", as_index=False).agg({
        "order_purchase_timestamp": "max", #mengambil tanggal order terakhir
        "order_id": "nunique",
        "payment_value": "sum"
    })
    rfm_df.columns = ["customer_id", "max_order_timestamp", "frequency", "monetary"]
    
    rfm_df["max_order_timestamp"] = rfm_df["max_order_timestamp"].dt.date
    recent_date = df["order_purchase_timestamp"].dt.date.max()
    rfm_df["recency"] = rfm_df["max_order_timestamp"].apply(lambda x: (recent_date - x).days)
    rfm_df.drop("max_order_timestamp", axis=1, inplace=True)
    
    return rfm_df

def create_bystate_seller_df(df):
    bystate_seller_df = df.groupby(by="seller_state").seller_id.nunique().reset_index()
    bystate_seller_df.rename(columns={
        "seller_id": "seller_count"
    }, inplace=True)
    
    return bystate_seller_df

def create_bycity_seller_df(df):
    bycity_seller_df = df.groupby(by="seller_city").seller_id.nunique().reset_index()
    bycity_seller_df.rename(columns={
        "seller_id": "seller_count"
    }, inplace=True)
    
    return bycity_seller_df

def create_seller_maps_df(df):
    seller_maps_df = geopandas.GeoDataFrame(
    df, geometry=geopandas.points_from_xy(df.geolocation_lng, df.geolocation_lat), crs="EPSG:4326"
    )

    return seller_maps_df


def create_rfm_seller_df(df):
    rfm_seller_df = df[df["order_status"] == "delivered"].groupby(by="seller_id", as_index=False).agg({
        "order_purchase_timestamp": "max", #mengambil tanggal order terakhir
        "order_id": "nunique",
        "payment_value": "sum"
    })
    rfm_seller_df.columns = ["seller_id", "max_order_timestamp", "frequency", "monetary"]
    
    rfm_seller_df["max_order_timestamp"] = rfm_seller_df["max_order_timestamp"].dt.date
    recent_date = df["order_purchase_timestamp"].dt.date.max()
    rfm_seller_df["recency"] = rfm_seller_df["max_order_timestamp"].apply(lambda x: (recent_date - x).days)
    rfm_seller_df.drop("max_order_timestamp", axis=1, inplace=True)
    
    return rfm_seller_df

GMV_penjualan_df = pd.read_csv("GMV_penjualan.csv")
orders_product_df = pd.read_csv("product_orders.csv")
orders_review_df = pd.read_csv("review_orders.csv")
orders_customer_df = pd.read_csv("customer_orders.csv")
orders_seller_df = pd.read_csv("seller_orders.csv")

datetime_columns = ["order_purchase_timestamp", "order_delivered_customer_date"]
GMV_penjualan_df.sort_values(by="order_purchase_timestamp", inplace=True)
GMV_penjualan_df.reset_index(inplace=True)
orders_product_df.sort_values(by="order_purchase_timestamp", inplace=True)
orders_product_df.reset_index(inplace=True)
orders_review_df.sort_values(by="order_purchase_timestamp", inplace=True)
orders_review_df.reset_index(inplace=True)
orders_customer_df .sort_values(by="order_purchase_timestamp", inplace=True)
orders_customer_df.reset_index(inplace=True)
orders_seller_df.sort_values(by="order_purchase_timestamp", inplace=True)
orders_seller_df.reset_index(inplace=True)
 
for column in datetime_columns:
    GMV_penjualan_df[column] = pd.to_datetime(GMV_penjualan_df[column])
    orders_product_df[column] = pd.to_datetime(orders_product_df[column])
    orders_review_df[column] = pd.to_datetime(orders_review_df[column])
    orders_customer_df[column] = pd.to_datetime(orders_customer_df[column])
    orders_seller_df[column] = pd.to_datetime(orders_seller_df[column])
 
min_date = GMV_penjualan_df["order_purchase_timestamp"].min()
max_date = GMV_penjualan_df["order_purchase_timestamp"].max()
 
with st.sidebar:
    # Menambahkan logo perusahaan
    st.image("https://miro.medium.com/v2/resize:fit:500/1*CRi_TRcr4IFDoTB68-ndew.png")
    
    # Mengambil start_date & end_date dari date_input
    start_date, end_date = st.date_input(
        label='Rentang Waktu',min_value=min_date,
        max_value=max_date,
        value=[min_date, max_date]
    )

main_df = GMV_penjualan_df[(GMV_penjualan_df["order_purchase_timestamp"] >= str(start_date)) & 
                (GMV_penjualan_df["order_purchase_timestamp"] <= str(end_date))]
main_df_1 = orders_product_df[(orders_product_df["order_purchase_timestamp"] >= str(start_date)) & 
                (orders_product_df["order_purchase_timestamp"] <= str(end_date))]
main_df_2 = orders_customer_df[(orders_customer_df["order_purchase_timestamp"] >= str(start_date)) & 
                (orders_customer_df["order_purchase_timestamp"] <= str(end_date))]
main_df_3 = orders_review_df[(orders_review_df["order_purchase_timestamp"] >= str(start_date)) & 
                (orders_review_df["order_purchase_timestamp"] <= str(end_date))]
main_df_4 = orders_seller_df[(orders_seller_df["order_purchase_timestamp"] >= str(start_date)) & 
                (orders_seller_df["order_purchase_timestamp"] <= str(end_date))]

daily_orders_df = create_daily_orders_df(main_df)
sum_order_items_df = create_sum_order_items_df(main_df_1)
sum_order_reviews_df = create_sum_order_reviews_df(main_df_3)
bystate_df = create_bystate_df(main_df_2)
bycity_df = create_bycity_df(main_df_2)
customer_maps_df = create_customer_maps_df(main_df_2)
rfm_df = create_rfm_df(main_df_2)
bystate_seller_df = create_bystate_seller_df(main_df_4)
bycity_seller_df = create_bycity_seller_df(main_df_4)
seller_maps_df = create_seller_maps_df(main_df_4)
rfm_seller_df = create_rfm_seller_df(main_df_4)

# plot number of daily orders (2021)
st.title('Olist Collection Dashboard :sparkles:')
st.header('Daily Orders')

col1, col2 = st.columns(2)

with col1:
    total_orders = daily_orders_df.order_count.sum()
    st.metric("Total orders", value=total_orders)

with col2:
    total_GMV = format_currency(daily_orders_df.GMV.sum(), "BRL", locale='pt_BR') 
    st.metric("Total GMV", value=total_GMV)

fig, ax = plt.subplots(figsize=(16, 8))
ax.plot(
    daily_orders_df["order_purchase_timestamp"],
    daily_orders_df["order_count"],
    marker='o', 
    linewidth=2,
    color="#90CAF9"
)
ax.tick_params(axis='y', labelsize=20)
ax.tick_params(axis='x', labelsize=15)

st.pyplot(fig)

# Product performance
st.header("Best & Worst Performing Product")

st.markdown("#### Performance Based on Sales")
fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(35, 15))

colors = ["#90CAF9", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3"]

sns.barplot(x="order_id", y="product_category_name_english", data=sum_order_items_df.head(5), palette=colors, ax=ax[0])
ax[0].set_ylabel(None)
ax[0].set_xlabel("Number of Sales", fontsize=30)
ax[0].set_title("Best Performing Product", loc="center", fontsize=50)
ax[0].tick_params(axis='y', labelsize=35)
ax[0].tick_params(axis='x', labelsize=30)

sns.barplot(x="order_id", y="product_category_name_english", data=sum_order_items_df.sort_values(by="order_id", ascending=True).head(5), palette=colors, ax=ax[1])
ax[1].set_ylabel(None)
ax[1].set_xlabel("Number of Sales", fontsize=30)
ax[1].invert_xaxis()
ax[1].yaxis.set_label_position("right")
ax[1].yaxis.tick_right()
ax[1].set_title("Worst Performing Product", loc="center", fontsize=50)
ax[1].tick_params(axis='y', labelsize=35)
ax[1].tick_params(axis='x', labelsize=30)

st.pyplot(fig)

# Product review
st.markdown("#### Performance Based on Reviews")
fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(24, 6))

colors = ["#72BCD4", "#D3D3D3", "#D3D3D3", "#D3D3D3", "#D3D3D3"]

sns.barplot(x="mean_score_review", y="product_category", data=sum_order_reviews_df.head(5), palette=colors, hue="product_category", ax=ax[0])
ax[0].set_ylabel(None)
ax[0].set_xlabel(None)
ax[0].set_title("Best Product Category Reviews", loc="center", fontsize=18)
ax[0].tick_params(axis ='y', labelsize=15)

sns.barplot(x="mean_score_review", y="product_category", data=sum_order_reviews_df.sort_values(by="mean_score_review", ascending=True).head(5), palette=colors, hue="product_category", ax=ax[1])
ax[1].set_ylabel(None)
ax[1].set_xlabel(None)
ax[1].invert_xaxis()
ax[1].yaxis.set_label_position("right")
ax[1].yaxis.tick_right()
ax[1].set_title("Worst Product Category Reviews", loc="center", fontsize=18)
ax[1].tick_params(axis='y', labelsize=15)

plt.suptitle("Best and Worst Category Product Based on Reviews ", fontsize=20)
st.pyplot(fig)


st.header("Customer Demographics")
# Customer City & State
st.subheader("Customer City & State")
col1, col2 = st.columns(2)

with col1:
    fig, ax = plt.subplots(figsize=(20, 10))
    sns.barplot(
        x="customer_count", 
        y="customer_state",
        data=bystate_df.sort_values(by="customer_count", ascending=False).head(5),
        palette=colors,
        ax=ax
    )
    ax.set_title("Number of Customer by States", loc="center", fontsize=30)
    ax.set_ylabel(None)
    ax.set_xlabel(None)
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=15)
    st.pyplot(fig)

with col2:
    fig, ax = plt.subplots(figsize=(20, 10))
    sns.barplot(
        x="customer_count", 
        y="customer_city",
        data=bycity_df.sort_values(by="customer_count", ascending=False).head(5),
        palette=colors,
        ax=ax
    )
    ax.set_title("Number of Customer by City", loc="center", fontsize=30)
    ax.set_ylabel(None)
    ax.set_xlabel(None)
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=15)
    st.pyplot(fig)

# Customer Maps
st.subheader("Customer Maps")
# Reproject to Web Mercator (EPSG:3857) for basemap compatibility
customer_maps_df = customer_maps_df.to_crs(epsg=3857)
fig, ax = plt.subplots(figsize=(16, 8))
customer_maps_df.plot(ax=ax, marker='o', color='blue', markersize=5)
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
ax.set_title("Customer Locations with Basemap", fontsize=16)
st.pyplot(fig)

# Best Customer Based on RFM Parameters
st.subheader("Best Customer Based on RFM Parameters")

col1, col2, col3 = st.columns(3)

with col1:
    avg_recency = round(rfm_df.recency.mean(), 1)
    st.metric("Average Recency (days)", value=avg_recency)

with col2:
    avg_frequency = round(rfm_df.frequency.mean(), 2)
    st.metric("Average Frequency", value=avg_frequency)

with col3:
    avg_monetary= format_currency(rfm_df.monetary.mean(), "BRL", locale="pt_BR")
    st.metric("Average Monetary", value=avg_monetary)

fig, ax = plt.subplots(nrows=1, ncols=3, figsize=(30, 6))

colors = ["#72BCD4", "#72BCD4", "#72BCD4", "#72BCD4", "#72BCD4"]

sns.barplot(y="recency", x="customer_id", data=rfm_df.sort_values(by="recency", ascending=True).head(5), palette=colors, hue="customer_id", ax=ax[0])
ax[0].set_ylabel(None)
ax[0].set_xlabel(None)
ax[0].set_title("By Recency (days)", loc="center", fontsize=18)
ax[0].tick_params(axis ='x', rotation=90,labelsize=15)

sns.barplot(y="frequency", x="customer_id", data=rfm_df.sort_values(by="frequency", ascending=False).head(5), palette=colors, hue="customer_id", ax=ax[1])
ax[1].set_ylabel(None)
ax[1].set_xlabel(None)
ax[1].set_title("By Frequency", loc="center", fontsize=18)
ax[1].tick_params(axis='x', rotation=90,labelsize=15)

sns.barplot(y="monetary", x="customer_id", data=rfm_df.sort_values(by="monetary", ascending=False).head(5), palette=colors, hue="customer_id", ax=ax[2])
ax[2].set_ylabel(None)
ax[2].set_xlabel(None)
ax[2].set_title("By Monetary", loc="center", fontsize=18)
ax[2].tick_params(axis='x', rotation=90,labelsize=15)

plt.suptitle("Best Customer Based on RFM Parameters (customer_id)", fontsize=20)
st.pyplot(fig)



st.header("Seller Demographics")
# seller City & State
st.subheader("Seller City & State")
col1, col2 = st.columns(2)

with col1:
    fig, ax = plt.subplots(figsize=(20, 10))
    sns.barplot(
        x="seller_count", 
        y="seller_state",
        data=bystate_seller_df.sort_values(by="seller_count", ascending=False).head(5),
        palette=colors,
        ax=ax
    )
    ax.set_title("Number of Seller by States", loc="center", fontsize=30)
    ax.set_ylabel(None)
    ax.set_xlabel(None)
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=15)
    st.pyplot(fig)

with col2:
    fig, ax = plt.subplots(figsize=(20, 10))
    sns.barplot(
        x="seller_count", 
        y="seller_city",
        data=bycity_seller_df.sort_values(by="seller_count", ascending=False).head(5),
        palette=colors,
        ax=ax
    )
    ax.set_title("Number of Seller by City", loc="center", fontsize=30)
    ax.set_ylabel(None)
    ax.set_xlabel(None)
    ax.tick_params(axis='y', labelsize=20)
    ax.tick_params(axis='x', labelsize=15)
    st.pyplot(fig)

# Seller Maps
st.subheader("Seller Maps")
# Reproject to Web Mercator (EPSG:3857) for basemap compatibility
seller_maps_df = seller_maps_df.to_crs(epsg=3857)
fig, ax = plt.subplots(figsize=(16, 8))
seller_maps_df.plot(ax=ax, marker='o', color='blue', markersize=5)
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
ax.set_title("Seller Locations with Basemap", fontsize=16)
st.pyplot(fig)

# Best Seller Based on RFM Parameters
st.subheader("Best Seller Based on RFM Parameters")

col1, col2, col3 = st.columns(3)

with col1:
    avg_recency = round(rfm_seller_df.recency.mean(), 1)
    st.metric("Average Recency (days)", value=avg_recency)

with col2:
    avg_frequency = round(rfm_seller_df.frequency.mean(), 2)
    st.metric("Average Frequency", value=avg_frequency)

with col3:
    avg_monetary= format_currency(rfm_df.monetary.mean(), "BRL", locale="pt_BR")
    st.metric("Average Monetary", value=avg_monetary)

fig, ax = plt.subplots(nrows=1, ncols=3, figsize=(30, 6))

colors = ["#72BCD4", "#72BCD4", "#72BCD4", "#72BCD4", "#72BCD4"]

sns.barplot(y="recency", x="seller_id", data=rfm_seller_df.sort_values(by="recency", ascending=True).head(5), palette=colors, hue="seller_id", ax=ax[0])
ax[0].set_ylabel(None)
ax[0].set_xlabel(None)
ax[0].set_title("By Recency (days)", loc="center", fontsize=18)
ax[0].tick_params(axis ='x', rotation=90,labelsize=15)

sns.barplot(y="frequency", x="seller_id", data=rfm_seller_df.sort_values(by="frequency", ascending=False).head(5), palette=colors, hue="seller_id", ax=ax[1])
ax[1].set_ylabel(None)
ax[1].set_xlabel(None)
ax[1].set_title("By Frequency", loc="center", fontsize=18)
ax[1].tick_params(axis='x', rotation=90,labelsize=15)

sns.barplot(y="monetary", x="seller_id", data=rfm_seller_df.sort_values(by="monetary", ascending=False).head(5), palette=colors, hue="seller_id", ax=ax[2])
ax[2].set_ylabel(None)
ax[2].set_xlabel(None)
ax[2].set_title("By Monetary", loc="center", fontsize=18)
ax[2].tick_params(axis='x', rotation=90,labelsize=15)

plt.suptitle("Best Seller Based on RFM Parameters (seller_id)", fontsize=20)
st.pyplot(fig)



st.caption('Copyright Desember 2024')