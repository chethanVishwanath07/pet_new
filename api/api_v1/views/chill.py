from fastapi import APIRouter, File, UploadFile, Response
from ..services.invoices import chill, auspost, netlogix, commonutils
from ..services.aws import upload_file, get_file, downloadfile_from_s3_bucket
from ..services.filetype import Invoice_Type, Invoice_Subsection, Status, Invoice_Processing_Status

from fastapi import HTTPException
from fastapi.responses import JSONResponse

router = APIRouter()


@router.post("/upload_invoice", status_code=201)
async def upload_invoice(invoice_type: Invoice_Type, file: UploadFile = File(...)):
    """
      :param file:
      :return:
    """
   # try:
    file_exist = await commonutils.check_for_file(file.filename)

    if file_exist:
        print("if file exists")
        return JSONResponse(status_code=400, content={"status": "File already exists"})
    else:
        input_file = await file.read()
        print("reading file")
        if invoice_type == invoice_type.Auspost:
            await auspost.start_invoice_processing(input_file, file.filename)

        elif invoice_type == invoice_type.Chill:
            await chill.start_invoice_processing(input_file, file.filename)

        elif invoice_type == invoice_type.Netlogix:
            await netlogix.start_invoice_processing(input_file, file.filename)

        print("uploading file")
        await upload_file(file.filename, input_file)
        print("uploaded file - " + file.filename)
        return JSONResponse(status_code=200, content={"status" : "File uploaded successfully"})

    # except Exception as err:
    #      print(err)
    #      return Response(status_code=204, content= "File not found" )


@router.get("/invoice")
async def get_invoice_data(invoice_type: Invoice_Type):
    """
    to get invoice details
    :param invoice_type:
    :param subsection:
    :return:
    """
    result = await commonutils.get_invoices_received_data(invoice_type.value)
    return result


@router.get("/invoice/status")
async def get_invoice_status(invoice_type: Invoice_Type):
    """

    :param invoice_type:
    :return:
    """
    result = await commonutils.get_invoice_satus_data(invoice_type.value)
    return result


@router.get("/invoice/line_items")
async def get_invoice_line_item(invoice_id: int, invoice_type: Invoice_Type,
                                section: int = Invoice_Subsection.Default.value):
    """
    :param invoice_id:
    :param invoice_type:
    :param section:
    :return:
    """
    # section_value = -1
    # if section.value == "0":
    #     section_value = 0
    # elif section.value == "1":
    #     section_value = 1

    print("get invoice line items")
    if invoice_type == Invoice_Type.Auspost:
        result = auspost.get_invoice_line_items(invoice_id)
        return result
    elif invoice_type == Invoice_Type.Chill:
        result = chill.get_invoice_line_items(invoice_id, section)
        return result
    elif invoice_type == Invoice_Type.Netlogix:
        result = netlogix.get_invoice_line_items(invoice_id)
        return result


@router.get("/invoice/summary_variance")
async def get_invoice_summary_variance(invoice_id: int, invoice_type: Invoice_Type,
                                       subsection: int = Invoice_Subsection.Default.value):
    print("get variance")

    # add subsection
    if invoice_type == Invoice_Type.Auspost:
        result = auspost.get_invoice_variance_summary(invoice_id)
        return result
    elif invoice_type == Invoice_Type.Chill:
        result = chill.get_invoice_variance_summary(invoice_id, subsection)
        return result
    elif invoice_type == Invoice_Type.Netlogix:
        result = netlogix.get_invoice_variance_summary(invoice_id)
        return result

    return


@router.get("/invoice/netlogix_summary")
async def get_netlogix_sumaary(invoice_id: int):
    print("In get summary")
    result = netlogix.get_netlogix_summary(invoice_id)
    return result


@router.post("/invoice/update_status")
async def upload_invoice_status(invoice_id: int, status: Status):
    """

    :param invoice_id:
    :param status:
    :return:
    """
    print("In invoice status update")

    result = await commonutils.invoice_status_update(invoice_id, status.value)
    if result:
        # update processing status as well
        if status.value == Status.Approve:
            await commonutils.insert_invoice_processing_status(invoice_id,
                                                               Invoice_Processing_Status.Approved_for_payment)
        return {"status": "Status updated Successfully"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Status not updated")


@router.get("/get_invoice_file")
async def get_file_from_s3(invoice_id: int, invoice_type: Invoice_Type,
                           subsection: int = Invoice_Subsection.Default.value):
    """
   invoice_type: Invoice_Type, section: Invoice_Subsection

   :param invoice_id:
   :param invoice_type:
   :param section:
   :return:
   """

    #
    # print("in get file")
    file_name = await commonutils.get_invoice_file_name(invoice_id, invoice_type.value, subsection)
    f_name = file_name["invoice_path"]
    # return downloadfile_from_s3_bucket(f_name)
    # print(f_name)
    result = await get_file(f_name)
    media_type = ""
    file_name_split = f_name.split(".")
    if len(file_name_split) > 0:
        file_format = file_name_split[1]
        print(file_format)

        media_type = "application/" + file_format
        return Response(content=result, media_type=media_type)
    else:
        raise HTTPException(
            status_code=404,
            detail="File not found")


@router.get("/get_invoice_processing_status")
async def get_invoice_processing_status(invoice_id):
    print("Invoice process status")
    result = commonutils.select_invoice_processing_status((invoice_id))
    slist = []
    for item in result:
        slist.append(item["id"])

    return slist
