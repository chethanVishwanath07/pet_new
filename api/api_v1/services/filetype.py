from enum import Enum


class Invoice_Subsection(Enum):
    Sales_order = 0
    Consignments = 1
    Default = -1


class Invoice_Type(Enum):
    Auspost = "AUSPOST"
    Chill = "CHILL"
    Netlogix = "NETLOGIX"


class Status(Enum):
    Pending = "PENDING"
    Requires_Permission = "PERMISSION"
    Approve = "APPROVED"
    Reject = "REJECTED"


class Invoice_Processing_Status(Enum):
    Extracted = "EXTRACTED"
    Supplier_Match = "SUPPLIERMATCH"
    Order_Match =    "ORDERMATCH"
    Rate_card_Match =   "RATECARDMATCH"
    Ready_for_payment =  "READY"
    Approved_for_payment =   "APPROVEDFORPAYMENT"


# fuel surcharges for the month may and april move this to DB
class FuelSurcharge(Enum):
    March = 6.42
    April = 10.62
    May = 8.61
    June = 13.59

class AllowedFileExtension(Enum):
    PDF = "pdf"


