import os
import io
import requests
import json
import datetime
import csv
import base64
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import streamlit as st

  
load_dotenv()

# Escapes string for XML
def generate_escaped(string =""):

        if "&" in string:
            return string.replace("&","&amp;")
        elif "<" in string:
            return string.replace("<", "&lt;")
        else:
            return string

def process_df(df):
    
    # Group by Delivery Number, Product ID, and aggregate the Quantity
    df = df.groupby(['Order ID', 'Offer ID', 'Version'], as_index=False).agg({
        'Company Name': 'first',
        'Address 1': 'first',
        'Address 2': 'first',
        'Address 3': 'first',
        'City': 'first',
        'State': 'first',
        'Postal Code': 'first',
        'Country': 'first',
        'Quantity': 'sum',
        'Reference #': 'first',
        'Order Comments': 'first'
    })
    
    # Reorder columns
    df = df[['Order ID', 'Company Name', 'Address 1', 'Address 2', 'Address 3',
             'City', 'State', 'Postal Code', 'Country', 'Offer ID', 'Version', 'Quantity', 'Reference #',
             'Order Comments']]

    # Remove pandas index
    df = df.set_index('Order ID')
    df = df.fillna("")
    df = df.sort_values(by="Order ID", ascending=True)
    
    return df

# Orders class to generate XML API calls to VeraCore
class Orders:
    offers = []
    purchase_orders = []
    
    def __init__(self, user, passw, order_id= None):
        self.order_id = order_id
        self.offers = []
        self.user_id = user
        self.password = passw

    def add_to_offers(self, offer):
        self.offers.append(offer)

    # Iterates through added offers and creates the offer XML to be added
    def private_generate_offer_xml(self):

        offer_string = ""
        purchase_order_string = ""

        for index, offer in enumerate(self.offers):
            new_offer = f"""
                    <OfferOrdered>
                        <Offer>
                            <Header>
                                <ID>{generate_escaped(offer[9])}</ID>
                            </Header>
                        </Offer>
                        <Quantity>{int(offer[10])}</Quantity>
                        <OrderShipTo>
                            <Key>1</Key>
                        </OrderShipTo>
                    </OfferOrdered>"""
            offer_string += new_offer

            # Adds all the purchase order numbers to one string
            if not(offer[11] in self.purchase_orders):
                
                if index == len(self.offers)-1:
                    purchase_order_string += str(offer[11])
                else:
                    purchase_order_string += str(offer[11]) + ","
                
                self.purchase_orders.append(offer[11])
        

        return offer_string, purchase_order_string
    
    # Generates XML needed for VeraCore SOAP API Add Orders endpoint
    def generate_xml(self):
        offer_string, purchase_order_string = self.private_generate_offer_xml()

        return f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope
            xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <soap:Header>
                <AuthenticationHeader
                    xmlns="http://omscom/">
                    <Username>{generate_escaped(self.user_id)}</Username>
                    <Password>{generate_escaped(self.password)}</Password>
                </AuthenticationHeader>
            </soap:Header>
            <soap:Body>
                <AddOrder
                    xmlns="http://omscom/">
                    <order>
                        <Header>
                            <ID>{self.order_id}</ID>
                            <EntryDate>2025-07-16T00:00:00</EntryDate>
                            <Comments>{generate_escaped(self.offers[0][13])}</Comments>
                            <ReferenceNumber>{generate_escaped(purchase_order_string)}</ReferenceNumber>
                        </Header>
                        <Shipping>
                            <FreightCarrier>
                                <Name>{generate_escaped(self.offers[0][12])}</Name>
                            </FreightCarrier>
                            <NeededBy>{self.offers[0][15]}</NeededBy>
                        </Shipping>
                        <Money></Money>
                        <Payment></Payment>
                        <OrderedBy>
                            <CompanyName>{generate_escaped(self.offers[0][1])}</CompanyName>
                            <Address1>{generate_escaped(self.offers[0][2])}</Address1>
                            <Address2>{generate_escaped(self.offers[0][3])}</Address2>
                            <Address3>{generate_escaped(self.offers[0][4])}</Address3>
                            <City>{generate_escaped(self.offers[0][5])}</City>
                            <State>{self.offers[0][6]}</State>
                            <PostalCode>{self.offers[0][7]}</PostalCode>
                            <Country>{self.offers[0][8]}</Country>
                        </OrderedBy>
                        <ShipTo>
                            <OrderShipTo>
                                <Flag>OrderedBy</Flag>
                                <Key>1</Key>
                            </OrderShipTo>
                        </ShipTo>
                        <BillTo>
                            <Flag>OrderedBy</Flag>
                        </BillTo>
                        <Offers>
                            {offer_string} 
                        </Offers>
                    </order>
                </AddOrder>
            </soap:Body>
        </soap:Envelope>
        """

# Credentials Prompt
st.text("1. Provide your web user credentials")

# Web User Credentials 
user_id = st.text_input("Web User ID")
passer = st.text_input("Web User Pass", type="password")

if user_id == "" or passer == "":
    st.text("")
    st.text("")
    st.error("Please input the credentials needed as a web user", icon=":material/warning:")

st.text("")
st.text("")
st.text("")

# CSV Prompt
st.text("2. Upload your CSV")

# Header Text 
st.text("Your headers should include in order from 0->12:")

headers = ['Order ID', 'Company Name', 'Address 1', 'Address 2', 'Address 3',
             'City', 'State', 'Postal Code', 'Country', 'Offer ID',"Version", 'Quantity', 'Reference #',
            'Order Comments']


st.table(headers)

st.text("")
st.text("")
# File upload
uploaded_file = st.file_uploader('Upload your Order CSV file with Versions', type=['csv'])

if not(uploaded_file == None):
    uploaded_df = pd.read_csv(uploaded_file)
    uploaded_headers = uploaded_df.columns.to_list()

    st.text("")
    st.text("")
    headers_text = "The following headers are missing: \n\n"
    isHeaderMissing = False
    for header in headers:
        if not(header in uploaded_headers):
            headers_text += f"{header} \n"
            isHeaderMissing = True

    headers_text += "\nPlease upload the CSV with the correct headers."

    if isHeaderMissing:
        st.error(headers_text, icon=":material/warning:")
    else:
        order_df = uploaded_df[["Order ID", "Offer ID", "Version", "Quantity"]]
        st.text("Summarized Order Upload")
        st.dataframe(order_df)

        api_df = process_df(uploaded_df)

        order_rows = api_df.itertuples()

    








