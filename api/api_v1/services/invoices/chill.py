import numpy as np
import re
import os

from app.api.api_v1 import constants
from app.api.api_v1.services.Constants import chill_constants
from ..database import insert_invoice_data, insert_into_sales_order, insert_consignments, \
    select_sales_order_line_items, update_invoice_status, select_consignments_line_items, \
    select_chill_sales_order_variance_summary, selcet_chill_consignment_variance_summary, \
    insert_invoice_processing_status
from ..filetype import Invoice_Type, Status, Invoice_Subsection, Invoice_Processing_Status
from .commonutils import match_tor_data
import pandas as pd
from ..aws import getDataframeToPdfBase64, upload_file
from app.queue import job_queue


async def start_invoice_processing(file, filename):

    job = job_queue.enqueue(start_validation, args=(file, filename), job_timeout=3600)

    #start_validation(df_sales_order, df_consignments, invoice_id)
    return


def start_validation(file, filename):
    # read invoice data
    invoice_xls = pd.ExcelFile(file)
    df_sales_order = pd.read_excel(invoice_xls, chill_constants.SALES_ORDER_WORKSHEET)
    df_cover = pd.read_excel(invoice_xls, chill_constants.COVER_WORKSHEET)
    df_consignments = pd.read_excel(invoice_xls, chill_constants.CONSIGNMENTS_WORKSHEET)

    # read invoice summary and save it to db
    invoice_id = invoice_summary_data(df_cover, df_sales_order, filename)
    # after
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Extracted.value)
    # default suppier match
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Supplier_Match.value)


    chill_salesorder_validation(df_sales_order, invoice_id)
    # process consignments data
    chills_consignment_validation(df_consignments, invoice_id)
    # after the validation of both update the processing status
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Rate_card_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Order_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Ready_for_payment.value)
    return


def invoice_summary_data(df_cover, df_sales_order, filename):
    df_cover.fillna("")
    # df column1 has all the required values and the column is not named
    global invoice_id
    invoice_id = \
        re.findall(r'\d+', df_cover[df_cover["Unnamed: 1"].str.contains("Invoice", na=False)]["Unnamed: 1"].values[0])[
            0]

    vendor = constants.VENDOR

    new_header = df_sales_order.iloc[0]  # grab the first row as new the header
    df_sales_order.columns = new_header
    df_sales_order.columns = df_sales_order.columns.str.upper()
    print(df_sales_order.columns)
    total = df_sales_order.iloc[-1][chill_constants.SO_CHARGE]
    # insert invoice values to db
    # check date
    insert_invoice_data(invoice_id, total, vendor, Invoice_Type.Chill.value, Status.Pending.value, filename)
    return invoice_id


def chill_salesorder_validation(df_sales_order, invoice_id):
    description = ""
    df_sales_order[constants.CALCULATED] = 0.0
    df_sales_order[constants.RATE_MATCHED] = False
    df_sales_order[constants.TOR_MATCHED] = False
    df_sales_order[constants.DESCRIPTION] = ""
    df_sales_order[constants.SOURCE] = ""
    df_sales_order[constants.DESTINATION] = ""
    df_sales_order[constants.INVOICE_ID] = 0
    df_sales_order[constants.ROUTE_MATCHED] = False
    df_sales_order[constants.INVOICE_TYPE] = Invoice_Type.Chill.value
    df_sales_order = df_sales_order.iloc[1:-1]  # remove top header row and totalsum row from regex validation
    HITL = False
    # "Calculated", "Rate Matched","TOR Matched", "Description", "Source", "Destination", "Route Matched","Invoice ID"
    # business logic for sales order validation
    total_sum = 0
    for index, row in df_sales_order.iterrows():
        # no columns in sales order is matched against rate card
        df_sales_order.at[index, constants.RATE_MATCHED] = True
        per_unit_total = 0
        # get all the digits from charge description column that is 4
        values = re.findall(r'\d+(?:\.\d+)?', row[chill_constants.SO_CHARGE_DESCRIPTION])
        # sample value looks like -  Unit - (10 @ $0.80 each ($8.00).) Carton - (24 @ $0.80 each ($19.20).) EA - (59 @ $0.80 each ($47.20).)
        # Pallet Wrapping Price calculated as 2.00 x $5.00 = $10.00

        for i in range(0, len(values), 3):
            # take first 2 values and multiply to get the third value and sum them for each set of values
            try:
                per_unit_total += float(values[i]) * float(values[i + 1])
            except:
                HITL = True
                per_unit_total = 0
                description += constants.FORMAT_NOT_FOUND + ","
                df_sales_order.at[index, constants.RATE_MATCHED] = False
                continue

        # check if the value is same as value in the charge column
        if abs(round(per_unit_total, 3) - row[chill_constants.SO_CHARGE]) <= 0.01:
            HITL = True

        # update dataframe with the calculated value
        df_sales_order.at[index, constants.CALCULATED] = per_unit_total
        # keep total sum
        total_sum += per_unit_total
        # TOR number validation

        tor_no = row["REFERENCE"]
        result = match_tor_data(tor_no)
        if result[0] == True:
            df_sales_order.at[index, constants.TOR_MATCHED] = True
            df_sales_order.at[index, constants.SOURCE] = result[1]
            df_sales_order.at[index, constants.DESTINATION] = result[2]
            if result[3] in row["DELIVERY NAME"].upper():
                df_sales_order.at[index, constants.ROUTE_MATCHED] = True
        else:
            HITL = True
            description += result[1]

        df_sales_order.at[index, constants.DESCRIPTION] = ""
        df_sales_order.at[index, constants.INVOICE_ID] = invoice_id

    # change the date format
    df_sales_order["DATE ADDED"] = pd.to_datetime(df_sales_order["DATE ADDED"]).dt.strftime('%Y-%m-%d')
    # convert dataframe into list of tuples

    insert_values = list(df_sales_order.itertuples(index=False, name=None))
    insert_into_sales_order(insert_values)
    if HITL:
        update_invoice_status(invoice_id, Status.Requires_Permission.value)
    return

def setup_consignment_df(df_consignment):
    keywords_to_exclude = ["MANIFEST ", "AUTO", "TOTAL", "FUEL", "BASE"]
    search_for = ['RATES', 'MISC']
    consignments_column = 'Consignments and Manifests'
    reference = "REFERENCE"
    search_for = ['RATES', 'MISC']
    df_consignment.dropna(axis=0, how="all", inplace=True)
    df_consignment[consignments_column] = df_consignment[consignments_column].astype(str)
    df_consignment[consignments_column] = df_consignment[consignments_column].str.upper()
    df_consignment = df_consignment[
        ~df_consignment[consignments_column].str.contains('|'.join(keywords_to_exclude), na=False)]
    df_consignment.reset_index(drop=True, inplace=True)
    row_index = df_consignment.index[df_consignment[consignments_column] == reference].tolist()
    row_header = df_consignment.iloc[row_index[0], :]
    print(row_index)
    df_consignment.columns = row_header
    df_consignment.columns = df_consignment.columns.str.upper()
    df_consignment = df_consignment.drop(row_index)
    df_consignment.reset_index(drop=True, inplace=True)
    invoice_index = -1

    invoice_df = pd.DataFrame(
        columns=["REFERENCE", "CUSTOMER NAME", "SUBURB", "DELIVERY DATE", "# CTN", "# PLT", "CHARGE DESCRIPTION",
                 "BASE CHARGES", "FUEL CHARGES"])

    for index, row in df_consignment.iterrows():
        if any(row["REFERENCE"] in s for s in search_for):
            # print(row[[2,3, -1]])
            invoice_df.loc[invoice_index][[6, 7, 8]] = row[[2, -2, -1]]


        elif not "NAN" in row["REFERENCE"]:
            invoice_index += 1
            # print(row[[0, 1, 2, 3, 4, 5]])
            invoice_df.loc[invoice_index] = row[[0, 1, 2, 3, 4, 5]]

    # replace all null values to repective default values
    invoice_df[chill_constants.DELIVERY_DATE] = pd.to_datetime(invoice_df[chill_constants.DELIVERY_DATE], errors='coerce')
    invoice_df[chill_constants.DELIVERY_DATE] = (invoice_df[chill_constants.DELIVERY_DATE].astype(str).replace({'NaT': None}).replace(np.nan, None))
    invoice_df["BASE CHARGES"] = invoice_df["BASE CHARGES"].replace(np.nan, 0.0)
    invoice_df["FUEL CHARGES"] = invoice_df["FUEL CHARGES"].replace(np.nan, 0.0)
    invoice_df["CHARGE DESCRIPTION"] = invoice_df["CHARGE DESCRIPTION"].replace(np.nan, '')

    print(invoice_df)
    return invoice_df

def old (df_consignment):

    consignments_column = 'Consignments and Manifests'
    # covert column to str and fill na as empty str
    df_consignment[consignments_column] = df_consignment[consignments_column].astype(str)
    df_consignment[consignments_column] = df_consignment[consignments_column].fillna('')
    print(df_consignment.columns)


    # the row having refernce has required heading for TOR data, ge the index for new column header
    row_index = df_consignment.index[df_consignment[consignments_column] == chill_constants.MC_REFERENCE].tolist()[0]
    # this is new row header
    row_header = df_consignment.iloc[row_index, :]
    print(row_header)
    # filter rows having  TOR reference
    tor_search = ["TOR"]
    df_TORdata = df_consignment[df_consignment[consignments_column].str.contains('|'.join(tor_search), na=False)]
    df_TORdata.columns = row_header
    # extract necessary columns
    df_TORdata = df_TORdata[df_TORdata.columns[[0, 1, 2, 3, 4, 5]]]
    df_TORdata.reset_index(drop=True, inplace=True)
    # some dates are not date format
    # chage
    df_TORdata["DELIVERY DATE"] = pd.to_datetime(df_TORdata["DELIVERY DATE"], errors='coerce')
    # replace NAT with none
    df_TORdata['DELIVERY DATE'] = (df_TORdata['DELIVERY DATE'].astype(str).replace({'NaT': None}).replace(np.nan, None))
    # filter row having only rates  and misc
    search_for = ['RATES', 'MISC']
    df_description = df_consignment[df_consignment[consignments_column].str.contains('|'.join(search_for), na=False)]
    # drop all columns with null values
    df_description = df_description.dropna(axis=1, how="all")
    df_description = df_description[df_description.columns[[2, 3, 4]]]

    # add column names
    df_description.columns = [chill_constants.CHARGE_DESCRIPTION, chill_constants.BASE_CHARGES,
                              chill_constants.FUEL_CHARGES]
    df_description.reset_index(drop=True, inplace=True)

    df_consignment = pd.concat([df_TORdata, df_description], axis=1)
    # to ignore case set all column name to upper
    df_consignment.columns = df_consignment.columns.str.upper()


def chills_consignment_validation(df_consignment, invoice_id):
    df_chill_ratecard = pd.read_excel(os.getcwd() + "/app/api/api_v1/services/RateCards/CHILL_RATECARD.xlsx")
    df_chill_ratecard.columns = df_chill_ratecard.columns.str.upper()
    invoice_df = setup_consignment_df(df_consignment)


    # fill all null values with empty values
    invoice_df[chill_constants.MC_SUBURB].fillna('', inplace=True)

    invoice_df[chill_constants.PALLET_CALCULATED] = 0
    invoice_df[chill_constants.CARTON_CALCULATED] = 0
    invoice_df[constants.RATE_MATCHED] = True
    invoice_df[constants.TOR_MATCHED] = False
    invoice_df[constants.ROUTE_MATCHED] = False
    invoice_df[constants.DESCRIPTION] = ""
    invoice_df[constants.SOURCE] = ""
    invoice_df[constants.DESTINATION] = ""
    invoice_df[constants.INVOICE_ID] = 0
    invoice_df[constants.INVOICE_TYPE] = str(Invoice_Type.Chill.value)
    invoice_df[chill_constants.ZONE] = ""

    for index, row in invoice_df.iterrows():
        print(index)
        description = ""
        # add invoice id for each TOR
        invoice_df.at[index, constants.INVOICE_ID] = invoice_id
        # read suburb from this invoice row
        suburb = row[chill_constants.MC_SUBURB].upper()  # SUBURB
        # read pallet count from this invoice row
        pallet_count = row["# PLT"]
        # reat carton count from this invoice row
        carton_count = row["# CTN"]

        # read the rate card for the given suburb from chill_ratecard
        df_chill_ratecard["STORE NAME"] = df_chill_ratecard["STORE NAME"].str.upper()
        store_rate = df_chill_ratecard[df_chill_ratecard["STORE NAME"].str.contains(suburb)]
        # if the rate card for given suurb exists
        if len(store_rate) == 1:
            # once column found rate matching starts
            invoice_df.at[index, constants.RATE_MATCHED] = True

            # read the state which is in 1st column
            state = store_rate.iloc[0, 0].upper()

            # read the zone which is the 3rd column in rate card
            zone = store_rate.iloc[0, 2].upper()

            # pass these values to calculte carton price
            total_carton_amount = per_carton_validation(state, zone, pallet_count, carton_count)
            # per pallet price is the 8th column in the ratecard
            # calculate the total pallet price
            total_pallet_amount = pallet_count * store_rate.iloc[0, 7]
            total_calculated = (total_pallet_amount + total_carton_amount)
            total_in_invoice = row[chill_constants.BASE_CHARGES] + row[chill_constants.FUEL_CHARGES]
            # For each TOR  calculated values of per pallet, per carton and total

            invoice_df.loc[index, chill_constants.PALLET_CALCULATED] = total_pallet_amount
            invoice_df.loc[index, chill_constants.CARTON_CALCULATED] = total_carton_amount
            invoice_df.loc[index, chill_constants.ZONE] = zone
            if abs(round(total_calculated, 2) - total_in_invoice) >= 0.01:
                HITL = True
                description += constants.RATE_NOT_MATCHED

        elif len(store_rate) == 0:
            HITL = True
            invoice_df.at[index, constants.RATE_MATCHED] = False
            # description += constants.RATE_CARD_NOTFOUND + suburb + ","
            # convert dataframe into list of tuples for sql insert

        # Read TOR from this invoice
        tor_no = row["REFERENCE"]
        # from the ERP dump read the from and to code for this TOR
        result = match_tor_data(tor_no)
        if result[0]:
            invoice_df.at[index, constants.TOR_MATCHED] = True
            invoice_df.at[index, constants.SOURCE] = result[1]
            invoice_df.at[index, constants.DESTINATION] = result[2]
            if result[3].upper() in row[chill_constants.MC_SUBURB].upper():
                invoice_df.at[index, constants.ROUTE_MATCHED] = True
            else:
                HITL = True
                invoice_df.at[index, constants.ROUTE_MATCHED] = False
                # description += chill_constants.ROUTE_NOT_MATCHED + ","
        else:
            HITL = True
            # description += result[1]

        invoice_df.at[index, constants.DESCRIPTION] = ""

    insert_values = list(invoice_df.itertuples(index=False, name=None))
    print(insert_values[0])
    insert_consignments(insert_values)
    if HITL:
        update_invoice_status(invoice_id, Status.Requires_Permission.value)
    return


def setup_consignments(invoice_xls):
    df_consignments = pd.read_excel(invoice_xls, chill_constants.CONSIGNMENTS_WORKSHEET)
    consignements_column = 'Consignments and Manifests'
    # covert column to str and fill na as empty str
    df_consignments[consignements_column] = df_consignments[consignements_column].astype(str)
    df_consignments[consignements_column] = df_consignments[consignements_column].fillna('')

    # the row having refernce has required heading for TOR data, ge the index for new column header
    row_index = df_consignments.index[df_consignments["Consignments and Manifests"].str.
        contains("REFERENCE", na=False, case=False)].tolist()[0]
    # this is new row header
    row_header = df_consignments.iloc[row_index, :]

    # filter rows having  TOR reference
    df_TORdata = df_consignments[df_consignments["Consignments and Manifests"].str.contains("TOR", na=False)]
    df_TORdata.columns = row_header
    # extract necessary columns
    df_TORdata = df_TORdata[df_TORdata.columns[[0, 1, 2, 3, 4, 5]]]

    # filter row having only rates
    df_rates = df_consignments[df_consignments["Consignments and Manifests"].str.contains("RATES", na=False)]
    # drop all columns with null values
    df_rates = df_rates.dropna(axis=1, how="all")
    df_rates = df_rates[df_rates.columns[[2, 3, 4]]]

    # add column names
    df_rates.columns = [chill_constants.CHARGE_DESCRIPTION, chill_constants.BASE_CHARGES, chill_constants.FUEL_CHARGES]

    df_new = pd.concat([df_TORdata, df_rates], axis=1)
    df_new.columns = df_new.columns.str.upper()

    df_new[chill_constants.PALLET_CALCULATED] = 0
    df_new[chill_constants.CARTON_CALCULATED] = 0
    df_new[constants.SOURCE] = ""
    df_new[constants.DESTINATION] = ""
    df_new[constants.DESCRIPTION] = ""
    return df_new


def get_invoice_line_items(invoice_id, subsection):
    if subsection == Invoice_Subsection.Sales_order.value:
        result = select_sales_order_line_items(invoice_id)
        return result

    elif subsection == Invoice_Subsection.Consignments.value:
        print("in subsetion 1 ")
        result = select_consignments_line_items(invoice_id)
        return result


def get_invoice_variance_summary(invoice_id, subsection):
    if subsection == Invoice_Subsection.Sales_order.value:
        print("inside sales")
        result = select_chill_sales_order_variance_summary(invoice_id)
        return result
    elif subsection == Invoice_Subsection.Consignments.value:
        result = selcet_chill_consignment_variance_summary(invoice_id)
        return result


# this needs to be changed

def per_carton_validation(state, zone, pallet_rate, carton_count):
    # $45 < 6 CTN     +$3 per carton >6   (Capped at $90.00 / 30 CTNs) +$3 per carton >30
    # if pallet_rate == 0:
    #     return 0
    # else:
    #
    min_ctn = 0
    min_price = 0
    capped_price = 0
    capped_count = 0
    free_above = 0
    price_per_ctn = 3
    price_per_ctn_above_30 = 0
    total_price = 0

    if state == "QLD" or "NSW" or "VIC" or "WA" or "SA":
        # These readings are from the rate card needs to be changed
        if zone == "ZONE 1":
            min_ctn = 6
            min_price = 45
            capped_price = 90
            capped_count = 30
            price_per_ctn_above_30 = 3
        elif zone == "ZONE 2":
            min_ctn = 6
            min_price = 55
            capped_price = 100
            capped_count = 30
            price_per_ctn_above_30 = 3
        elif zone == "ZONE 3":
            min_ctn = 6
            min_price = 65
            capped_price = 110
            capped_count = 30
            price_per_ctn_above_30 = 3
            # for all the other regions and other 2 states ( TAS and NT) per carton price is NA.
        else:
            return 0

        free_above_ctn = (capped_price - min_price) / price_per_ctn + min_ctn

        if carton_count <= min_ctn:
            total_price = min_price

        elif carton_count > 6 and carton_count <= free_above_ctn:
            total_price = min_price + (carton_count - min_ctn) * 3

        elif carton_count > free_above_ctn and carton_count <= capped_count:
            total_price = capped_price

        else:
            total_price = capped_price + (carton_count - capped_count) * 3

    return total_price
