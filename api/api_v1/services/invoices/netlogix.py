import os, re, locale, datetime
import numpy as np
from ..aws import text_extract
import pandas as pd
from app.api.api_v1 import constants
from app.api.api_v1.services.Constants import netlogix_constants
from ..filetype import Invoice_Type, Status, FuelSurcharge, Invoice_Processing_Status
from .commonutils import match_tor_data
from ..database import update_invoice_status, insert_invoice_data_netlogix, insert_invoice_data, \
    insert_netlgix_summary_data, select_netlogix_line_items, select_netlogix_variance_summary, \
    select_invoice_summary_netlogix, insert_invoice_processing_status
from collections import defaultdict
from app.queue import job_queue


async def start_invoice_processing(file, file_name):
    print("Invoice process started")

    job = job_queue.enqueue(start_validation, args=(file, file_name), job_timeout=3600)
    return

    # add validation in queue process
    #start_validation(invoice_line_item_df, invoice_id, invoice_date)
    #return invoice_line_item_df, invoice_id, invoice_date


def start_validation(file, file_name):
    invoice_line_item_df, invoice_kvs = set_up_invoice(file)
    invoice_id, invoice_date = save_invoice_summary_data(invoice_kvs, file_name)
    # once data extracted update status
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Extracted.value)
    # default supplier match
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Supplier_Match.value)
    print("Invoice ID is {0}, Date is {1}", invoice_id, invoice_date)
    print("inside start validation")
    HITL = False
    fuel_surcharge = 0.0
    invoice_date = datetime.datetime.strptime(invoice_date.strip(), "%d %b %Y")
    # # fuel surcharges
    if invoice_date.month == 3:
        fuel_surcharge = FuelSurcharge.March.value
    elif invoice_date.month == 4:
        fuel_surcharge = FuelSurcharge.April.value
    elif invoice_date.month == 5:
        fuel_surcharge = FuelSurcharge.May.value
    elif invoice_date.month == 6:
        fuel_surcharge = FuelSurcharge.June.value

    rates_df, qty_break_limit = set_up_rate_card()
    dead_weight: int = 1000
    cubic_conversion: int = 250
    rate_value: float = 0.0
    # # exclude total maout col from calculations
    invoice_line_item_df = invoice_line_item_df.iloc[0:-3]
    invoice_line_item_df[constants.INVOICE_ID] = invoice_id
    for index, row in invoice_line_item_df.iterrows():
        amount = 0
        print("Line item index ", index, row[netlogix_constants.EXCL_GST])
        try:
            amount = round(float(remove_format_float(row[netlogix_constants.EXCL_GST])), 2)
        except Exception as er:
            print("Failed to convert ", amount)
            invoice_line_item_df.at[index, constants.RATE_MATCHED] = False
            HITL = True
            continue

        invoice_line_item_df.at[index, netlogix_constants.EXCL_GST] = amount

        print(row[netlogix_constants.GOODS_DESCRIPTION])
        # if wait time
        if netlogix_constants.DEMURRAGED_PRODUCT_DESCRIPTION in row[netlogix_constants.GOODS_DESCRIPTION]:
            print("Demurraged")
            invoice_line_item_df.at[index, netlogix_constants.IS_DEMURRAGED] = True
            invoice_line_item_df.at[index, constants.CALCULATED] = amount
        else:
            invoice_line_item_df.at[index, netlogix_constants.IS_DEMURRAGED] = False
            try:
                weight = float(row[netlogix_constants.WEIGHT])
            except Exception as er:
                print("failed to convert weight", weight)
                weight = 0.0
                invoice_line_item_df.at[index, constants.RATE_MATCHED] = False
                HITL = True
                continue
            try:
                cube = float(row[netlogix_constants.CUBE])
            except Exception as er:
                print("failed to convert cube")
                invoice_line_item_df.at[index, constants.RATE_MATCHED] = False
                HITL = True
                continue

            x = weight * dead_weight
            y = cube * cubic_conversion
            qty_break_value = x if x > y else y
            col_index = get_qty_break_index(qty_break_value, qty_break_limit)
            postal_code = int(row[netlogix_constants.DESTZONE].split(",")[1])

            selected_rate = rates_df[rates_df["Receiver Post Code"] == postal_code]
            if len(selected_rate) == 1:
                invoice_line_item_df.at[index, netlogix_constants.DEST_POSTAL_CODE] = postal_code
                invoice_line_item_df.at[index, constants.RATE_MATCHED] = True
                rate_value = float(selected_rate.iloc[0, col_index])
                calculated_amt = rate_value * qty_break_value

                # add fuel_surcharge percentage
                calculated_amt = calculated_amt + (fuel_surcharge / 100) * calculated_amt

                invoice_line_item_df.at[index, constants.CALCULATED] = round(calculated_amt, 2)
                if abs(round(calculated_amt, 2) - amount) >= 0.01:
                    HITL = True
            else:
                invoice_line_item_df.at[index, constants.RATE_MATCHED] = False
                invoice_line_item_df.at[index, constants.CALCULATED] = 0.0
                HITL = True

        tor_no = row["CONSIGNMENT #"].replace("PB", "")
        # from the ERP dump read the from and to code for this TOR
        result = match_tor_data(tor_no)
        if result[0]:
            invoice_line_item_df.at[index, constants.TOR_MATCHED] = True
            invoice_line_item_df.at[index, netlogix_constants.FROM_STATE] = result[1]
            invoice_line_item_df.at[index, netlogix_constants.TO_STATE] = result[2]
            if result[3].upper() in row[netlogix_constants.DESTZONE].upper():
                invoice_line_item_df.at[index, constants.ROUTE_MATCHED] = True
            else:
                HITL = True
                invoice_line_item_df.at[index, constants.ROUTE_MATCHED] = False
        else:
            HITL = True
        # replace all zero with null values
        invoice_line_item_df[netlogix_constants.DEST_POSTAL_CODE] = invoice_line_item_df[
            netlogix_constants.DEST_POSTAL_CODE].replace({0: None}).replace(np.nan, 0.0)

    ##
    insert_values = list(invoice_line_item_df.itertuples(index=False, name=None))
    # update all the processing status
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Rate_card_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Order_Match.value)
    insert_invoice_processing_status(invoice_id, Invoice_Processing_Status.Ready_for_payment.value)

    print(insert_values)
    insert_invoice_data_netlogix(insert_values)

    if not HITL:
        pass
    else:
        update_invoice_status(invoice_id, Status.Requires_Permission.value)

    return


def set_up_invoice(file):
    response_list = extract_line_item_table_forms(file)
    dataframe_list, invoice_kvs = get_data_from_response(response_list)
    print("toatl data frames in invoice {0}".format(len(dataframe_list)))

    invoice_line_item_df = pd.DataFrame()

    i = 0
    while i < len(dataframe_list):
        print(dataframe_list[i].shape)
        invoice_line_item_df = pd.concat([invoice_line_item_df, dataframe_list[i]], axis=0)
        print("dataframe at ", i)
        print(dataframe_list[i])
        i += 1

    print(invoice_line_item_df)
    # change all values in first column to upper for case-insensitive comparison
    invoice_line_item_df = invoice_line_item_df.reset_index(drop=True)
    print(invoice_line_item_df)

    # search for "collection date" in oth column
    row_header_index = \
        invoice_line_item_df.index[invoice_line_item_df.iloc[:, 0].str.contains("Collection Date")]

    print("row header index ", row_header_index)
    # set that row as df column index

    row_header_index = row_header_index[0] if len(row_header_index) > 0 else -1

    invoice_line_item_df.columns = invoice_line_item_df.iloc[row_header_index]
    invoice_line_item_df.columns = invoice_line_item_df.columns.str.upper()
    # remove all unwanted lines from before collection date
    index_to_drop = 0
    if row_header_index > 0:
        index_to_drop = [*range(row_header_index + 1 )]
    else:
        index_to_drop = [0]

    print("values to drop",index_to_drop )
    print(row_header_index)
    print(invoice_line_item_df)
    invoice_line_item_df = invoice_line_item_df.drop(index_to_drop)
    print(invoice_line_item_df)
    # reset index
    invoice_line_item_df = invoice_line_item_df.reset_index(drop=True)

    invoice_line_item_df[constants.CALCULATED] = 0.0
    invoice_line_item_df[constants.RATE_MATCHED] = False
    invoice_line_item_df[constants.ROUTE_MATCHED] = False
    invoice_line_item_df[constants.TOR_MATCHED] = False
    invoice_line_item_df[constants.INVOICE_ID] = 0
    invoice_line_item_df[constants.INVOICE_TYPE] = Invoice_Type.Netlogix.value
    invoice_line_item_df[netlogix_constants.FROM_STATE] = ""
    invoice_line_item_df[netlogix_constants.TO_STATE] = ""
    invoice_line_item_df[netlogix_constants.IS_DEMURRAGED] = False
    invoice_line_item_df[netlogix_constants.DEST_POSTAL_CODE] = 0
    invoice_line_item_df.columns = invoice_line_item_df.columns.str.upper()

    # replace all nan with 0 for integer columns

    invoice_line_item_df[netlogix_constants.EXCL_GST] = invoice_line_item_df[netlogix_constants.EXCL_GST].replace('', 0)
    invoice_line_item_df[netlogix_constants.CUBE] = invoice_line_item_df[netlogix_constants.CUBE].replace('', 0)
    invoice_line_item_df[netlogix_constants.COUNT] = invoice_line_item_df[netlogix_constants.COUNT].replace('', 0)
    invoice_line_item_df[netlogix_constants.WEIGHT] = invoice_line_item_df[netlogix_constants.WEIGHT].replace('', 0)

    print("invoice set up done")
    return invoice_line_item_df, invoice_kvs


def search_values(invoice_kvs, search_key):
    for kvs in invoice_kvs:
        for key, value in kvs.items():
            if re.search(search_key, key, re.IGNORECASE):
                return value


def set_up_rate_card():
    rate_card_file = pd.ExcelFile(os.getcwd() + "/app/api/api_v1/services/RateCards/NETLOGIX_RATECARD.xlsx")
    rates_df = pd.read_excel(rate_card_file)

    # qty breaks are in first 2 rows
    qty_breaks = rates_df.loc[0:1]
    # drop nan values
    qty_breaks.dropna(axis=1, inplace=True)
    qty_break_limit = []
    # extarct columns to get QTY break limits
    for col in qty_breaks:
        qty_break_limit.append(qty_breaks[col])
    # update the rate card with column header in row 2
    rates_df.columns = rates_df.loc[2]
    # remove unwanted rows
    rates_df = rates_df.loc[3:]
    # reset index
    rates_df.reset_index(drop=True)
    return rates_df, qty_break_limit


def save_invoice_summary_data(invoice_kvs, file_name):
    get_value = lambda val: val[0] if not val is None else ""

    invoice_id = int(get_value(search_values(invoice_kvs, netlogix_constants.INVOICE_NUMBER)))
    invoice_date = get_value(search_values(invoice_kvs, netlogix_constants.INVOICE_DATE))
    account = get_value(search_values(invoice_kvs, netlogix_constants.ACCOUNT))
    abn_number = get_value(search_values(invoice_kvs, netlogix_constants.ABN_NUMBER))
    due_date = get_value(search_values(invoice_kvs, netlogix_constants.DUE_DATE))
    invoice_total = get_value(search_values(invoice_kvs, netlogix_constants.GST_EXCLUSIVE))
    bsb = get_value(search_values(invoice_kvs, netlogix_constants.BSB))
    account_no = get_value(search_values(invoice_kvs, netlogix_constants.ACCOUNT_NO))
    invoice_address = get_value(search_values(invoice_kvs, netlogix_constants.TAX_INVOICE))
    netlogix_address = "Netlogix Australia Pty Limited"
    phone_num = "+61 413 287 785"
    payment_advice = "DIRECT CREDIT"
    note = "Please reference name and invoice number"
    bank_account_name = "Netlogix Australia Pty Limited NAB"

    # insert invoice summary extarcted data and  common invoice data for all invoices
    # decimal_point_char = locale.localeconv()['decimal_point']
    # clean = re.sub(r'[^0-9' + decimal_point_char + r']+', '', str(invoice_total))
    invoice_total = remove_format_float(invoice_total)

    print(invoice_id, invoice_date, invoice_total)
    insert_netlgix_summary_data(invoice_id, invoice_date, account, abn_number, due_date, invoice_address,
                                netlogix_address, phone_num, payment_advice, note, bank_account_name, bsb, account_no)
    insert_invoice_data(invoice_id, invoice_total, constants.VENDOR, Invoice_Type.Netlogix.value, Status.Pending.value,
                        file_name)

    return invoice_id, invoice_date


def remove_format_float(value):
    decimal_point_char = locale.localeconv()['decimal_point']
    un_formatted = re.sub(r'[^0-9' + decimal_point_char + r']+', '', str(value))
    return un_formatted


def get_qty_break_index(value, qty_break_limit):
    index = -1

    # ignore first values
    i = 1
    while i < len(qty_break_limit):
        if float(qty_break_limit[i][0]) <= value <= float(qty_break_limit[i][1]):
            index = i
        i += 1
        # 3 to accommodate the first 3 columns for search
    return index + 3


def extract_line_item_table_forms(file):
    """
    :param file:
    :return:
    """
    # get form & tables both info
    respose_list = text_extract(
        file=file,
        feature_types=['TABLES', 'FORMS']
    )

    return respose_list


def get_data_from_response(response_list):
    line_items = []
    form_items = []

    def map_blocks(blocks, block_type):
        return {
            block['Id']: block
            for block in blocks
            if block['BlockType'] == block_type
        }

    for response in response_list:
        blocks = response['Blocks']
        tables = map_blocks(blocks, 'TABLE')
        cells = map_blocks(blocks, 'CELL')
        words = map_blocks(blocks, 'WORD')
        selections = map_blocks(blocks, 'SELECTION_ELEMENT')

        for table in tables.values():

            # Determine all the cells that belong to this table
            table_cells = [cells[cell_id] for cell_id in get_children_ids(table)]

            # Determine the table's number of rows and columns
            n_rows = max(cell['RowIndex'] for cell in table_cells)
            n_cols = max(cell['ColumnIndex'] for cell in table_cells)
            content = [[None for _ in range(n_cols)] for _ in range(n_rows)]

            # Fill in each cell
            for cell in table_cells:
                cell_contents = [
                    words[child_id]['Text']
                    if child_id in words
                    else selections[child_id]['SelectionStatus']
                    for child_id in get_children_ids(cell)
                ]
                i = cell['RowIndex'] - 1
                j = cell['ColumnIndex'] - 1
                content[i][j] = ' '.join(cell_contents)

            # We assume that the first row corresponds to the column names
            dataframe = pd.DataFrame(content[:])
            line_items.append(dataframe)

        # for extracting form key values data ,
        key_map = {}
        value_map = {}
        block_map = {}
        # redundant  for loop need to change
        for block in blocks:
            block_id = block['Id']
            block_map[block_id] = block
            if block['BlockType'] == "KEY_VALUE_SET":
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block

        kvs = get_kv_relationship(key_map, value_map, block_map)
        form_items.append(kvs)

    return line_items, form_items


def get_kv_relationship(key_map, value_map, block_map):
    kvs = defaultdict(list)
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs[key].append(val)
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '

    return text


def get_children_ids(block):
    for rels in block.get('Relationships', []):
        if rels['Type'] == 'CHILD':
            yield from rels['Ids']


def get_invoice_line_items(invoice_id):
    result = select_netlogix_line_items(invoice_id)
    return result


def get_invoice_variance_summary(invoice_id):
    result = select_netlogix_variance_summary(invoice_id)
    return result


def get_netlogix_summary(invoice_id):
    result = select_invoice_summary_netlogix(invoice_id)
    return result
