import pandas as pd
import os
from ..database import insert_invoice_data, update_invoice_status, insert_invoice_data_auspost, \
    select_auspost_line_items, select_auspost_variance_summary, insert_invoice_processing_status
from ..filetype import Invoice_Type, Status ,Invoice_Processing_Status
from app.api.api_v1 import constants
from app.api.api_v1.services.Constants import auspost_constants
from app.api.api_v1.constants import FROM_POSTAL_CODE_NOTFOUND, TO_POSTAL_CODE_NOTFOUND
from app.queue import job_queue
import math

async def start_invoice_processing(file, file_name):


    invoice_df = setup_invoice(file)
    # read invoice id and invoice date from first row
    invoice_id = invoice_df.at[0, auspost_constants.INVOICE_ID]
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Extracted.value)
    insert_invoice_processing_status(invoice_id,Invoice_Processing_Status.Supplier_Match.value)
    total = sum(invoice_df[auspost_constants.AMT_INCL_TAX])
    vendor = constants.VENDOR

    # invoice_id, total,vendor,  invoicetype , status, location , invoice_date)
    print("insert invoice data")
    insert_invoice_data(invoice_id, total, vendor, Invoice_Type.Auspost.value, Status.Pending.value, file_name)

    job = job_queue.enqueue(start_validation,args = (invoice_id, invoice_df),  job_timeout=1200)

    return


def setup_invoice(file):
    invoice_df = pd.read_excel(file)
    print("inside auspost set up")
    invoice_df = invoice_df[
        (invoice_df[auspost_constants.FROM_STATE] == "NSW")
        & (invoice_df[auspost_constants.TO_STATE] == "NSW")
        & (invoice_df[auspost_constants.BILLED_WEIGHT]) > 0]

    # reset index for ease of access
    invoice_df.reset_index(drop=True, inplace=True)
    # change all column header to upper
    invoice_df.columns = invoice_df.columns.str.upper()
    # extract necessary columns from the invoice
    invoice_df = invoice_df[
        [auspost_constants.CUSTOMER, auspost_constants.REGION,
         auspost_constants.FROM_STATE, auspost_constants.TO_STATE,
         auspost_constants.FROM_POSTAL_CODE, auspost_constants.TO_POSTAL_CODE,
         auspost_constants.AMT_INCL_TAX, auspost_constants.AMT_EXL_TAX,
         auspost_constants.CONSIGNMENT_ID, auspost_constants.ARTICLE_ID,
         auspost_constants.BILLING_DATE, auspost_constants.BILLED_WEIGHT,
         auspost_constants.INVOICE_ID
         ]]

    invoice_df[auspost_constants.CAL_INCL_TAX] = 0.0
    invoice_df[auspost_constants.CAL_EXCL_TAX] = 0.0
    invoice_df[constants.DESCRIPTION] = ""
    invoice_df[auspost_constants.ARTICLE_ID_MATCHED] = False
    # set to false, change it to true on matched conditions
    invoice_df[constants.ROUTE_MATCHED] = False
    invoice_df[constants.INVOICE_TYPE] = Invoice_Type.Auspost.value
    # set True by default, only in special conditions set it to false
    invoice_df[constants.RATE_MATCHED] = True
    return invoice_df


def start_validation(invoice_id, invoice_df):
    HITL = False
    auspost_order_post_df = set_up_auspost_order_post()
    area_definitions_df, zone_definitions_df, rates_df = set_up_rate_card()

    for index, row in invoice_df.iterrows():
        description = ""
        from_area = None
        to_area = None
        print(index)

        if pd.isna(row[auspost_constants.FROM_POSTAL_CODE]):
            description += "FROM_POSTAL_CODE  is null"

        else:
            # read necessary values from the invoice

            article_id = row[auspost_constants.ARTICLE_ID]
            from_postal_code = row[auspost_constants.FROM_POSTAL_CODE]
            to_postal_code = row[auspost_constants.TO_POSTAL_CODE]
            # round up the billed weight
            billed_weight = math.ceil(row[auspost_constants.BILLED_WEIGHT])

            amt_incl_tax = row[auspost_constants.AMT_INCL_TAX]
            amt_excl_tax = row[auspost_constants.AMT_EXL_TAX]
            # print(article_id,from_postal_code, to_postal_code, billed_weight, amt_incl_tax, amt_excl_tax)
            from_details = retrieve_postcode_details(area_definitions_df, from_postal_code)
            to_details = retrieve_postcode_details(area_definitions_df, to_postal_code)

            if not from_details is None:
                from_area = from_details["Area"]
            else:
                HITL = True
                description += FROM_POSTAL_CODE_NOTFOUND + ","
                invoice_df.at[index, constants.RATE_MATCHED] = False
                continue
            if not to_details is None:
                to_area = to_details["Area"]
            else:
                description += str(to_postal_code) + TO_POSTAL_CODE_NOTFOUND + ","
                invoice_df.at[index, constants.RATE_MATCHED] = False
                continue

            print(billed_weight)

            zone_tuple = zone_definitions_df[from_area][to_area]
            zone_tuple_list = zone_tuple.split(",")
            state_type = zone_tuple_list[0]
            lodgement = zone_tuple_list[1]
            print(state_type, lodgement )
            rates = rates_df[(rates_df["STATE TYPE"] == state_type) & (rates_df["LDGEMENT"] == lodgement) & (
                    rates_df["RATE"] == from_area)]
            incl_rates = rates[rates["GST_INCLUSIVE"] == 1]
            excl_rates = rates[rates["GST_INCLUSIVE"] == 0]
            calculated_incl_amt = get_rates(incl_rates, billed_weight)
            calculated_excl_amt = get_rates(excl_rates, billed_weight)
            invoice_df.at[index, auspost_constants.CAL_INCL_TAX] = calculated_incl_amt
            invoice_df.at[index, auspost_constants.CAL_EXCL_TAX] = calculated_excl_amt
            # update once rates are found and matched
            invoice_df.at[index, constants.RATE_MATCHED] = True
            # print(article_id,from_postal_code, to_postal_code,billed_weight, calculated_incl_amt, calculated_excl_amt)
            # print(amt_incl_tax,amt_excl_tax)

            if (amt_excl_tax - round(calculated_excl_amt, 2)) <= 0.01:
                HITL = True
            if (amt_incl_tax - round(calculated_incl_amt, 2)) <= 0.01:
                HITL = True

            if len(auspost_order_post_df[auspost_order_post_df["AWB"] == article_id]) == 1:
                # if article id found
                invoice_df.at[index, auspost_constants.ARTICLE_ID_MATCHED] = True
                order_auspost_index = index = \
                    d_index = auspost_order_post_df.index[[auspost_order_post_df["AWB"] == article_id]].values[0]
                m3_dest_postal_code = auspost_order_post_df.at[d_index, "Destination Postcode"]
                if m3_dest_postal_code == to_postal_code:
                    invoice_df.at[index, constants.ROUTE_MATCHED] = True
                else:
                    invoice_df.at[index, constants.ROUTE_MATCHED] = False

            else:
                invoice_df.at[index, auspost_constants.ARTICLE_ID_MATCHED] = False
                description += auspost_constants.ARTICLE_ID_NOT_FOUND
                invoice_df.at[index, constants.ROUTE_MATCHED] = False
                HITL = True

        invoice_df.at[index, constants.DESCRIPTION] = description

    insertvalues = list(invoice_df.itertuples(index=False, name=None))
    print(insertvalues)
    insert_invoice_data_auspost(insertvalues)


    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Rate_card_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Order_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Ready_for_payment.value)

    if HITL:
        update_invoice_status(invoice_id, Status.Requires_Permission.value)

    return



def set_up_auspost_order_post():
    order_post_file = pd.ExcelFile(os.getcwd() + "/app/api/api_v1/services/RateCards/ORDER_REPORT_AUSPOST.xlsx")
    order_post_df = pd.read_excel(order_post_file, auspost_constants.ORDER_REPORT_WORKSHEET)

    # row index is in different column fix that
    row_index = order_post_df.index[order_post_df["Order Report"] == "Order Number"].tolist()[0]
    row_header = order_post_df.iloc[row_index, :]
    order_post_df.columns = row_header
    order_post_df = order_post_df.iloc[3:]
    order_post_df.reset_index(drop=True)
    return order_post_df


def set_up_rate_card():
    AUS_Post_Ratecard = pd.ExcelFile(os.getcwd() + "/app/api/api_v1/services/RateCards/AUSPOST_RATECARD.xlsx")
    area_defintions_df = pd.read_excel(AUS_Post_Ratecard, auspost_constants.AREA_DEFINITION_WORKSHEET)
    zone_definitions_df = pd.read_excel(AUS_Post_Ratecard, auspost_constants.ZONE_DEFINITION_WORKSHEET)
    rates_df = pd.read_excel(AUS_Post_Ratecard, auspost_constants.RATES_WORKSHEET)
    zone_definitions_df.set_index("Destinations", inplace=True)
    return area_defintions_df, zone_definitions_df, rates_df


def retrieve_postcode_details(area_defintions_df, areacode):
    for index, row in area_defintions_df.iterrows():

        post_codes = row["Postcode"].split(",")
        for code in post_codes:

            code_range = code.split('-')
            if (areacode >= int(code_range[0])) & (areacode <= int(code_range[1])):
                return row

    return None


def get_rates(rates, billed_weight):
    # less than 500 grams
    if billed_weight <= 0.5:
        # rate column is 2,
        rate = rates.iloc[0, 2]

    elif billed_weight <= 1:
        # ratecolumn is 3
        rate = rates.iloc[0, 3]

    elif billed_weight <= 3:
        rate = rates.iloc[0, 4]

    elif billed_weight <= 5:
        rate = rates.iloc[0, 5]

    # more than 5
    else:
        rate = rates.iloc[0, 6] + billed_weight * rates.iloc[0, 8]


    return rate


def get_invoice_line_items(invoice_id):
    result = select_auspost_line_items(invoice_id)
    return result


def get_invoice_variance_summary(invoice_id):
    result = select_auspost_variance_summary(invoice_id)
    return result
