from fastapi import APIRouter
from .views import chill,graphs


router = APIRouter()
router.include_router(chill.router, prefix="/invoiceprocess", tags=["invoices"])
router.include_router(graphs.router, prefix="/dashboard", tags =["dashboard"])

