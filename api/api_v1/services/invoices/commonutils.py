import pandas as pd
import os
import json
from ..database import select_invoices_received_data, select_invoices_status_data,\
update_invoice_status,select_invoice_path,select_invoice_processing_status, insert_invoice_processing_status,check_if_file_exists


from app.api.api_v1 import constants


def match_tor_data(torno):
    description = ""
    # read TOR dump file
    TRO_excel = pd.ExcelFile(os.getcwd() + "/app/api/api_v1/services/RateCards/NAV TOR Data Dump.xlsx")

    df_tro_dump = pd.read_excel(TRO_excel, "TOR_No")  # TOR dump
    # from the ERP dump read the from and to code for this TOR

    df_from_code = pd.read_excel(TRO_excel, "From_Code")  # read the codes for source
    df_to_code = pd.read_excel(TRO_excel, "To_Code")  # read the codes for destination

    # read data for TOR reference
    tor_data = df_tro_dump[df_tro_dump["No."] == torno]
    if len(tor_data) == 1:
        from_code = tor_data["Transfer-from Code"]
        if len(from_code) == 1:
            from_code = from_code.values[0]
        else:
            description += constants.FROM_POSTAL_CODE_NOTFOUND + ","

        to_code = tor_data["Transfer-to Code"]
        if len(to_code) == 1:
            to_code = to_code.values[0]
        else:
            description += constants.TO_POSTAL_CODE_NOTFOUND + ","

        # update source and destination
        source = df_from_code[df_from_code["DC No"] == from_code]["State"].values[0]
        destination = str(df_to_code[df_to_code["Store No"] == to_code]["State"].values[0]).upper()
        storename = str(df_to_code[df_to_code["Store No"] == to_code]["Store Name"].values[0]).upper()

        return True, source, destination, storename
    else:
        description += constants.TOR_NOTFOUND
        return False, description


async def get_invoices_received_data(invoice_type):
    print("inside commomutils")
    invoice_received_list = select_invoices_received_data(invoice_type)
    return invoice_received_list


async def get_invoice_satus_data(invoice_type):
    invoice_status = select_invoices_status_data(invoice_type)
    return invoice_status


async def invoice_status_update(invoice_id, status):
    print("update")

    result = update_invoice_status(invoice_id,status)
    return result


async def get_invoice_procesing_status(invoice_id):
    result = select_invoice_processing_status(invoice_id)
    return result


async def insert_invoice_processing_status(invoice_id):
    result = insert_invoice_processing_status(invoice_id)
    return result


async def get_invoice_file_name(invoice_id, invoice_type , subsection):
    print("get file name")
    path = select_invoice_path(invoice_id)
    return path


async def check_for_file(file_name):
     count = check_if_file_exists(file_name)
     print(count)
     if count[0]["count"] >= 1:
         return True
     else:
         return False
