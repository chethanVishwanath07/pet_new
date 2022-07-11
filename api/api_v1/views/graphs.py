from fastapi import APIRouter
# from api.api_v1.services.invoices.constseries import get_constseries_info
from app.api.api_v1.services import database
# get_comparison_price_per_pallet, get_variances_found_query,get_spend_by_suppliers_query
# from app import main
router = APIRouter()

@router.get("/invoices_processed", status_code=200)
async def get_invoices_processed():
    result = await database.get_invoices_processed_query()
    return result


@router.get("/variances_found", status_code=200)
async def get_variances_found():
    result = await database.get_variances_found_query()
    return result


@router.get("/comparison_price_per_pallet", status_code=200)
async def get_comparison_price_pallet():
    result = await database.get_price_comparison_price_per_pallet()
    return result


@router.get("/total_spend_by_supplier",status_code=200)
async def get_total_spend_by_suppliers(source_state,destination_state):
    result = await database.get_total_spend_by_suppliers_query(source_state,destination_state)
    return result


@router.get("/chill_average_price_per_carton",status_code=200)
async def get_chill_average_price_per_carton(source_state,destination_state):
    result = await database.get_chill_average_price_per_carton_query(source_state,destination_state)
    return result

@router.get("/chill_average_price_per_pallet",status_code=200)
async def get_chill_average_price_per_pallet(source_state,destination_state):
    result = await database.get_chill_average_price_per_pallet_query(source_state,destination_state)
    return result 

@router.get("/chill_average_cost_per_order",status_code=200)
async def get_chill_average_cost_per_order(from_date,to_date,state_from):
    result = await database.get_chill_average_cost_per_order_query(from_date,to_date,state_from)
    return result  

@router.get("/netlogix_average_cost",status_code=200)
async def get_netlogix_average_cost(place_from,place_to):
    result = await database.get_netlogix_average_cost_query(place_from,place_to)
    return result      

@router.get("/auspost_average_costper_order",status_code=200)
async def get_auspost_average_cost_per_order(from_date,to_date,state_from):
    result = await database.get_auspost_average_costper_order(from_date,to_date,state_from)
    return result  
