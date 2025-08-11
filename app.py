import os
import re
import requests
import json
import datetime
import csv
import base64
import pandas as pd
import streamlit as st
from io import StringIO
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
  
load_dotenv()

# Application Client ID
client_id = os.getenv("CLIENT_ID")
# Tenant ID for Microsoft Entra
tenant_id = os.getenv("TENANT_ID")
# Secret value generated in the application
client_secret = os.getenv("ENTRA_CLIENT_SECRET")
# Scope
scope = os.getenv("SCOPE")
# Authority
authority = f"https://login.microsoftonline.com/{tenant_id}"

# Resideo user id
reporting_id = os.getenv("USER")
# Resideo inbox id
inbox_id = os.getenv("INBOX_FOLDER")

# VeraCore Web User/Pass/System
veracore_id = os.getenv("VERACORE_USER")
veracore_pass = os.getenv("VERACORE_PASS")

# Converts date to a string VeraCore can use
def convert_date(date_string: str):

    first_pattern = re.compile(r"[0-9]{4}[0-9]{2}[0-9]{2}")

    second_pattern = re.compile(r"[0-9]{2}/[0-9]{2}/[0-9]{4}")

    if first_pattern.search(date_string):
        return datetime.datetime.strptime(date_string,"%Y%m%d").strftime("%Y-%m-%dT%H:%M:%S")
    elif second_pattern.search(date_string):
        return datetime.datetime.strptime(date_string,"%m/%d/%Y").strftime("%Y-%m-%dT%H:%M:%S")

# Escapes string for XML
def generate_escaped(string =""):
        if "&" in string:
            return string.replace("&","&amp;")
        elif "<" in string:
            return string.replace("<", "&lt;")
        else:
            return string

# Writes Microsoft errors to error log
def write_to_log(text):
    path = os.getcwd()
    with open(path+"/"+"errors.txt", "a") as file:
        file.write(datetime.datetime.now().strftime("--------%m-%d-%yT%H:%M:%S----------------\n\n"))
        file.write(text)

# Validate version consistency and return validation results
def validate_version_consistency(df):
    """
    Validates that versions are consistent within each order and for each product.
    Returns a tuple: (is_valid, error_messages, cleaned_df)
    """
    validation_errors = []
    
    # Check for version consistency within each Order ID
    order_version_issues = []
    product_version_issues = []
    
    for order_id in df['Order ID'].unique():
        order_data = df[df['Order ID'] == order_id].copy()
        
        # Get unique versions for this order (excluding NaN/empty)
        order_versions = order_data['Version'].dropna()
        order_versions = order_versions[order_versions.astype(str).str.strip() != '']
        order_versions = order_versions[order_versions.astype(str).str.lower() != 'nan']
        unique_versions = order_versions.unique()
        
        # If there are multiple versions in the same order, that's an issue
        if len(unique_versions) > 1:
            order_version_issues.append({
                'order_id': order_id,
                'versions': list(unique_versions)
            })
        
        # Check for version consistency within each product in this order
        for offer_id in order_data['Offer ID'].unique():
            if pd.isna(offer_id) or str(offer_id).strip() == '':
                continue
                
            product_data = order_data[order_data['Offer ID'] == offer_id]
            product_versions = product_data['Version'].dropna()
            product_versions = product_versions[product_versions.astype(str).str.strip() != '']
            product_versions = product_versions[product_versions.astype(str).str.lower() != 'nan']
            unique_product_versions = product_versions.unique()
            
            if len(unique_product_versions) > 1:
                product_version_issues.append({
                    'order_id': order_id,
                    'offer_id': offer_id,
                    'versions': list(unique_product_versions)
                })
    
    # Generate error messages
    if order_version_issues:
        for issue in order_version_issues:
            validation_errors.append(
                f"Order {issue['order_id']} has inconsistent versions: {', '.join(map(str, issue['versions']))}"
            )
    
    if product_version_issues:
        for issue in product_version_issues:
            validation_errors.append(
                f"Order {issue['order_id']}, Product {issue['offer_id']} has inconsistent versions: {', '.join(map(str, issue['versions']))}"
            )
    
    is_valid = len(validation_errors) == 0
    
    return is_valid, validation_errors, df


# Group Order Dataframe
def process_df(df):

    is_valid, validation_errors, validated_df = validate_version_consistency(df)
    if not is_valid:
        # Return the validation errors as part of an exception or error object
        error_msg = "Version consistency validation failed:\n" + "\n".join(validation_errors)
        raise ValueError(error_msg)
    
    # For each Order ID + Offer ID combination, ensure we use a consistent version
    # We'll use the first non-null version found for each combination
    def get_consistent_version(group):
        versions = group['Version'].dropna()
        versions = versions[versions.astype(str).str.strip() != '']
        versions = versions[versions.astype(str).str.lower() != 'nan']
        if len(versions) > 0:
            return versions.iloc[0]  # Use the first valid version
        return None
    
    # Group by Order ID and Offer ID to ensure version consistency per product per order
    df_with_consistent_versions = df.groupby(['Order ID', 'Offer ID']).apply(
        lambda group: group.assign(Version=get_consistent_version(group))
    ).reset_index(drop=True)

    # Group by Delivery Number, Product ID, and aggregate the Quantity
    df = df_with_consistent_versions.groupby(['Order ID', 'Offer ID', 'Version'], as_index=False).agg({
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
    
    def __init__(self, user : str, passw, order_id= None):
        self.order_id : str= order_id
        self.offers = []
        self.versions = []
        self.purchase_orders = []
        self.user_id = user
        self.password = passw

    def add_to_offers(self, offer):
        self.offers.append(offer)

    # Iterates through added offers and creates the offer XML to be added
    def private_generate_offer_xml(self):

        offer_string = ""
        purchase_order_string = ""

        versions_in_order = set()
        for offer in self.offers:
            version = offer[10]  # Version is at index 10
            if version and str(version).strip() and str(version).strip().lower() != "nan":
                versions_in_order.add(str(version).strip())

        if len(versions_in_order) > 1:
            raise ValueError(f"Order {self.order_id} has inconsistent versions: {', '.join(versions_in_order)}")
                        
        for index, offer in enumerate(self.offers):
            (
                order_id, company, addr1, addr2, addr3,
                city, state, postal_code, country,
                offer_id, version, quantity, ref_number,
                comments
            ) = offer
            if not offer_id:
                continue

            new_offer = f"""
                    <OfferOrdered>
                        <Offer>
                            <Header>
                                <ID>{generate_escaped(offer_id)}</ID>
                            </Header>
                        </Offer>
                        <Quantity>{int(quantity)}</Quantity>
                        <OrderShipTo>
                            <Key>1</Key>
                        </OrderShipTo>
                    </OfferOrdered>"""
            offer_string += new_offer
            
            version_json = {
                "productId" : f"{offer_id}",
                "quantityToShip" : int(quantity)
            }

            if version and str(version).strip() and str(version).strip().lower() != "nan":
                version_json["version"] = str(version).strip()

            self.versions.append(version_json)

            # Adds all the purchase order numbers to one string
            if ref_number and ref_number not in self.purchase_orders:
                self.purchase_orders.append(str(ref_number))

        purchase_order_string = ",".join(self.purchase_orders)

        return offer_string, purchase_order_string
    
    # Generates XML needed for VeraCore SOAP API Add Orders endpoint
    def generate_order_xml(self):
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
                            <EntryDate>{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}</EntryDate>
                            <Comments>{generate_escaped(self.offers[0][13])}</Comments>
                            <ReferenceNumber>{generate_escaped(purchase_order_string)}</ReferenceNumber>
                        </Header>
                        <Money></Money>
                        <Payment></Payment>
                        <OrderedBy>
                            <CompanyName>{generate_escaped(self.offers[0][1])}</CompanyName>
                            <Address1>{generate_escaped(self.offers[0][2])}</Address1>
                            <Address2>{generate_escaped(self.offers[0][3])}</Address2>
                            <Address3>{generate_escaped(self.offers[0][4])}</Address3>
                            <City>{generate_escaped(self.offers[0][5])}</City>
                            <State>{generate_escaped(self.offers[0][6])}</State>
                            <PostalCode>{generate_escaped(self.offers[0][7])}</PostalCode>
                            <Country>{generate_escaped(self.offers[0][8])}</Country>
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
    
    def generate_version_json(self):
        return json.dumps({
            "orderId" : self.order_id,
            "warehouseId" : "3plwhs",
            "holdShippingOrder" : False,
            "products" : self.versions
        })

class Email:
    email_id = None
    # Email JSON needed for Microsoft Graph API

    email_json =  {
        "subject" : "Inbound Version  Order Errors",
        "body":{
            "contentType" : "HTML",
        },
    }

    # Empty method
    def generate_email():
        return ""

# Child of Email class for Error Email
class ErrorEmail(Email):
    # A dictionary that uses order ID for keys and error text for value

    def __init__(self):
        self.offers = []
        self.error_dict = {}
        self.hasError = False

        # Sends this to the ITs email
        self.email_json["toRecipients"] = [
            {
                "emailAddress" : {
                    "address" : "reporting@3plwinner.com"
                }
            }
        ]

    # Add error message to the error_dict under the order id
    def add_to_body(self, order_id, error_message):

        if not(self.error_dict.get(order_id) is None):
            self.error_dict[order_id] += "\n\n"
            self.error_dict[order_id] += error_message
        else:
            self.error_dict[order_id] = error_message

    def generate_email(self):

        date_string = datetime.datetime.now().strftime("%m-%d-%Y at %H:%M")

        # Adds new subject to error email
        self.email_json["subject"] = f"Inbound Version Order Errors {date_string}"

        body_html = ""

        # Iterates through the order ids and creates the html for an error code
        for order in self.error_dict.keys():
            errors = self.error_dict[order]

            body_html += f"<p><u>The order with ID {order}</u><p>"
            body_html += f"<p>Had the following errors:<p>"
            body_html += f"<p>{errors}</p>"
            body_html += "<br>"
        
        # Adds to the body of the email JSON
        self.email_json['body']['content'] = body_html
        
        return self.email_json

    # Adds offers 
    def add_offers(self, offers):     
        for offer in offers:
            self.offers.append(offer)
    
    # Generates bytes for CSV attachment
    def generate_error_bytes(self):
        # Inserts the headers as the first tuple
        self.offers.insert(0,('Delivery Number', 'Company Name/Contact Name', 'Address 1', 'Address 2', 'Address 3',
             'City', 'State', 'Postal Code', 'Country', 'Product ID', 'Quantity', 'Sales Order',
             'Shipping Conditions', 'Delivery Instructions', 'Carrier', 'Planned Ship Date'))

        # Creates an IO object and adds it to the CSV writer
        attachment_string = StringIO()

        csv_writer = csv.writer(attachment_string)

        # Writes each tuple in csv format
        for offer in self.offers:
            csv_writer.writerow(offer)

        csv_string = attachment_string.getvalue()

        # Encodes to base64 and gets the string to add to the JSON body
        encoded_csv = base64.b64encode(csv_string.encode("utf-8"))
        encoded_string = encoded_csv.decode("utf-8")

        attachment_string.close()

        return encoded_string

class ErrorObject:
    
    def __init__(self):
        self.is_error = False
        self.error_text = ""


# Gets authorization token for VeraCore REST API 
def get_auth(user :str, passw : str):
    endpoint = 'https://wms.3plwinner.com/VeraCore/Public.Api/api/Login'

    body = {
        "userName" : user,
        "password" : passw,
        "systemId" : "cus327"
    }

    response = requests.post(endpoint, data=body)

    if response.status_code == 403:
        return ({}, False)

    token = response.json()["Token"]

    os.environ["TOKEN"] = token

    auth_header = {
        "Authorization" : "bearer "+ token
    }

    return (auth_header, True)

def change_version(orders : Orders, error_email : ErrorEmail, auth_header, error_obj : ErrorObject):

    auth_header["Content-Type"] = "application/json"

    endpoint = 'https://wms.3plwinner.com/VeraCore/Public.Api/api/ShippingOrder'

    response = requests.post(endpoint, headers=auth_header, data=orders.generate_version_json())


    if not(response.status_code == 200):
        # If error we want to add the offers to the error email
        error_email.add_offers(orders.offers)

        error_text = response.json()["Error"]

        error_email.add_to_body(orders.order_id, error_text)

        # Marks that there was an error and to send an email
        error_email.hasError = True

        error_obj.is_error = True
        error_obj.error_text = "There was an issue changing the version of your order. The orders have now been sent to IT to investigate and upload."



# Makes API calls to create orders in VeraCore
def create_orders(orders: Orders, error_email : ErrorEmail, error_obj: ErrorObject):

    try:
        # Validate version consistency before making API calls
        versions_in_order = set()
        for offer in orders.offers:
            version = offer[10]  # Version is at index 10
            if version and str(version).strip() and str(version).strip().lower() != "nan":
                versions_in_order.add(str(version).strip())
        
        if len(versions_in_order) > 1:
            error_message = f"Order {orders.order_id} has inconsistent versions: {', '.join(versions_in_order)}"
            error_email.add_offers(orders.offers)
            error_email.add_to_body(orders.order_id, error_message)
            error_email.hasError = True
            error_obj.is_error = True
            error_obj.error_text = "Version consistency error detected. Orders have been sent to IT to investigate."
            return
        
    # Needs to be text/xml to work
        headers = {
            "Content-Type" : "text/xml"
        }

        response = requests.post("https://rhu335.veracore.com/pmomsws/OMS.asmx", headers=headers, data=orders.generate_order_xml())

        if response.status_code > 299:
        # If error, we want to add the offers to the error email
            error_email.add_offers(orders.offers)
            error_text = response.text
            split_string = error_text.split("System.Exception:")[-1]
            api_error = split_string.split("at")[0]

        # If the order already exists you just change the selected version on the order
            if "already exists" in api_error:
                auth_header, was_successful = get_auth(orders.user_id, orders.password)
            
            # If the auth was successful try to change the versions
                if was_successful:
                    change_version(orders,error_email,auth_header, error_obj)
                else:
                    error_obj.is_error = True
                    error_obj.error_text = "Invalid Credentials"
        # Add the credential error to the email and add the error text to the object
            else:
                error_email.add_to_body(orders.order_id, api_error)

            # Marks that there was an error and to send an email
                error_email.hasError = True

                error_obj.is_error = True
                error_obj.error_text = "There was an issue with one or more of your orders. The orders have now been sent to IT to investigate and upload."
    # Otherwise adding was successful and follow the same path
        else:
            auth_header, was_successful = get_auth(orders.user_id, orders.password)

            if was_successful:
                change_version(orders,error_email,auth_header, error_obj)
            else:
                error_obj.is_error = True
                error_obj.error_text = "Invalid Credentials"
    except ValueError as ve:
        # Handle version consistency errors
        error_email.add_offers(orders.offers)
        error_email.add_to_body(orders.order_id, str(ve))
        error_email.hasError = True
        error_obj.is_error = True
        error_obj.error_text = "Version consistency error detected. Orders have been sent to IT to investigate."

# Call back function/button submit function. Returns error email
def submit_orders(uploaded_df, error_obj : ErrorObject):
    try:
        api_df = process_df(uploaded_df)
    except ValueError as ve:
        # Handle version consistency validation errors
        error_obj.is_error = True
        error_obj.error_text = str(ve)
        error_email = ErrorEmail()
        error_email.hasError = True
        error_email.add_to_body("VALIDATION_ERROR", str(ve))
        return error_email


    # Get tuples to iterate through
    order_tuples = api_df.itertuples()

    # Create the first Orders object
    orders = Orders(user_id,passer,None)

    # Create an error email
    error_email = ErrorEmail()

    for order in order_tuples:

        # If the orders object is blank add order id
        if orders.order_id is None:
            orders.order_id = order[0]

        # If order IDs match add lines to the offers, otherwise send the API call and start on the next set of lines
        if orders.order_id == order[0]:
            orders.add_to_offers(order)
        else:    
            create_orders(orders,error_email, error_obj)

            # Create new orders object after creating order
            orders = Orders(user_id,passer,order[0])
            orders.add_to_offers(order)
    if orders.offers:
        create_orders(orders, error_email, error_obj)
    
    return error_email

# Generates an Outlook Draft email
def generate_outlook_email(user_id, email : Email, auth_header):
    generate_email_endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/"

    email_json = email.generate_email()

    response = requests.post(generate_email_endpoint, headers=auth_header, data=json.dumps(email_json))

    # If request is unsuccessful write to error log, otherwise return the draft id
    if not(response.status_code == 201):
        write_to_log(response.text)
        print(f"Draft wasn't created")
        return None
    else:
        print(f"Create draft : {response.status_code}")
        return response.json()["id"]

# Sends an Outlook draft
def send_outlook_email(user_id, draft_id, auth_header):
    send_draft_endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{draft_id}/send"

    response = requests.post(send_draft_endpoint, headers=auth_header)

    # If the request isn't successful write to a log
    if not(response.status_code == 202):
        write_to_log(response.text)
        print(f"Email wasn't sent")
    else:
        print(f"Send email : {response.status_code}")

# Creates a CSV attachment for a draft
def generate_attachment(user_id, email_id,csv_string, auth_header):
    attachment_endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{email_id}/attachments"

    # Attachment JSON needed
    attachment_body = {
    "@odata.type": "#microsoft.graph.fileAttachment",
    "name": "ErrorOffer.csv",
    "contentType": "text/csv",
    "contentBytes": csv_string
    }

    response = requests.post(attachment_endpoint,headers=auth_header, data=json.dumps(attachment_body))

    # If request is not successful write to log
    if not(response.status_code == 201):
        write_to_log(response.text)
        print(f"Attachment wasn't created")
    else:
        print(f"Create attachment : {response.status_code}")

# Create a confidential application to verify with MSAL
app = ConfidentialClientApplication(
    client_id=client_id,
    client_credential = client_secret,
    authority=authority
)

# Get OAuth token for application
result = app.acquire_token_for_client(scopes=[scope])

# Create auth header
masl_auth_header = {
    "Authorization" : f"Bearer {result["access_token"]}",
    "Content-Type" : "application/json"
}

# Credentials Prompt
st.text("1. Provide your web user credentials")

# Web User Credentials 
user_id = st.text_input("Web User ID")
passer = st.text_input("Web User Pass", type="password")

# Generate error if they have not submitted credentials
if user_id == "" or passer == "":
    st.text("")
    st.text("")
    st.error("Please input the credentials needed as a web user", icon=":material/warning:")
else:
    # Allows them to test their web credentials before submitting their orders
    st.text("")
    test = st.button(label="Test Credentials")

    if test:
        _,is_user = get_auth(user_id,passer)

        if is_user:
            st.success("These credentials work")
        else:
            st.error("There was an issue with these credentials")



st.text("")
st.text("")
st.text("")

# CSV Prompt
st.text("2. Upload your CSV")

# Header Text 
st.text("Your headers should include in order from 0->12:")

# Headers needed for the CSV
headers = ['Order ID', 'Company Name', 'Address 1', 'Address 2', 'Address 3',
             'City', 'State', 'Postal Code', 'Country', 'Offer ID',"Version", 'Quantity', 'Reference #',
            'Order Comments']

st.table(headers)

st.text("")
st.text("")

# File upload
uploaded_file = st.file_uploader('Upload your Order CSV file with Versions', type=['csv'])

if not(uploaded_file == None):

    # Read uploaded CSV to a panda
    uploaded_df = pd.read_csv(uploaded_file, dtype={"Postal Code" :str})
    
    # Get headers to check
    uploaded_headers = uploaded_df.columns.to_list()

    st.text("")
    st.text("")
    headers_text = "The following headers are missing: \n\n"
    is_header_missing = False

    # Check to see if any column headers are missing
    for header in headers:
        if not(header in uploaded_headers):
            headers_text += f"{header} \n"
            is_header_missing = True

    headers_text += "\nPlease upload the CSV with the correct headers."

    # If any are display error text
    if is_header_missing:
        st.error(headers_text, icon=":material/warning:")
    else:
        # Otherwise display the summarized order import
        order_df = uploaded_df[["Order ID", "Offer ID", "Version", "Quantity"]]
        st.text("Summarized Order Upload")
        st.dataframe(order_df)

        st.text("")
        st.text("")
        st.text("")
        # Generate boolean when the button is clicked
        ready = st.button("Submit")
        
        # Create an error object to use in the on the ready function
        error_obj = ErrorObject()
        
        if ready:
            # If button is clicked try submitting orders
            error_email = submit_orders(uploaded_df, error_obj)

            # If an error was found show error text and generate email with the orders
            if error_obj.is_error:
                st.text("")
                st.text("")
                st.text("")
                st.error(error_obj.error_text, icon=":material/warning:")

                if error_email.hasError:
                    # Generate error email
                    email_id = generate_outlook_email(reporting_id,error_email,masl_auth_header)

                    if email_id:

                        # Generate error attachment
                        generate_attachment(reporting_id,email_id,error_email.generate_error_bytes(),masl_auth_header)

                        # Send the email
                        send_outlook_email(reporting_id,email_id,masl_auth_header)
            
            # Otherwise display  success text
            else:
                st.text("")
                st.text("")
                st.text("")
                st.success("Your orders have been successfully uploaded with the correct version!")