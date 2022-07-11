from fastapi import HTTPException
# from app.api.api_v1.config import Settings
from ..config import get_settings
import psycopg2
import psycopg2.extras as p
from ..constants import FILEPATH


def get_ps_connection():
    try:
        print("----------")
        Settings = get_settings()
        print(Settings.DB_NAME, Settings.DB_HOST, Settings.DB_USERNAME)
        ps_connection = psycopg2.connect(
            database=Settings.DB_NAME,
            user=Settings.DB_USERNAME,
            password=Settings.DB_PASSWORD,
            host=Settings.DB_HOST,
            port=5432  # get_settings().DB_PORT
        )
        return ps_connection

    except Exception as err:
        print("DB connection error ", err)
        raise HTTPException(status_code=500, detail="Internel server error")


def execute_insert_query(query, many=False, values=[]):
    """

    :param values:
    :param many:
    :param query: m
    :return:
    """

    try:
        ps_connection = get_ps_connection()
        ps_connection.autocommit = True
        ps_cursor = ps_connection.cursor()
        if many:
            #ps_cursor.executemany(query, values)
            p.execute_batch(ps_cursor, query, values)

        else:
            ps_cursor.execute(query)
        ps_cursor.close()
        ps_connection.close()
        return True
    except Exception as err:

        print("DB connection error ", err)
        raise HTTPException(status_code=500, detail="Internel server error")


def execute_select_query(query):
    try:
        ps_connection = get_ps_connection()
        ps_connection.autocommit = True
        ps_cursor = ps_connection.cursor()
        ps_cursor.execute(query)
        query_result = ps_cursor.fetchall()
        print("Result -----------")
        print(query_result)
        query_result = convert_to_dict(ps_cursor.description, query_result)
        ps_cursor.close()
        ps_connection.close()
        return query_result
    except Exception as err:
        print("DB connection error %s", str(err))
        raise HTTPException(status_code=500, detail="Internel server error")


def execute_update_query(query):
    try:
        ps_connection = get_ps_connection()
        ps_connection.autocommit = True
        ps_cursor = ps_connection.cursor()
        ps_cursor.execute(query)
        print("updated rows {0} ".format(ps_cursor.rowcount))
        ps_cursor.close()
        ps_connection.close()
        return True
    except Exception as err:
        print("DB connection error ", err)
        raise HTTPException(status_code=500, detail="Internel server error")


def convert_to_dict(columns, results):
    """
    This method converts the resultset from postgres to dictionary
    interates the data and maps the columns to the values in result set and converts to dictionary
    :param columns: List - column names return when query is executed
    :param results: List / Tupple - result set from when query is executed
    :return: list of dictionary- mapped with table column name and to its values
    """

    allResults = []
    columns = [col.name for col in columns]
    if type(results) is list:
        for value in results:
            allResults.append(dict(zip(columns, value)))
        return allResults
    elif type(results) is tuple:
        allResults.append(dict(zip(columns, results)))
        return allResults


def check_if_file_exists(file_name):
    query = """
    select count(*) 
    from public.invoice_data
    where invoice_path ='{0}' 
    """.format(FILEPATH  + file_name )
    print(query)
    result = execute_select_query(query)
    return result


def insert_invoice_data(invoice_id, total, vendor, invoice_type, status, file_name):
    location = FILEPATH + file_name
    query = """INSERT INTO public.invoice_data(
	invoice_id, total, vendor, uploaded_date, invoice_type_id, status_id, invoice_path) 
    VALUES      
    ({0}, {1}, '{2}', now(),
    (select id from invoice_type_lk where invoice_type_code =  '{3}'), 
    (select id from invoice_status_lk where status_type_code = '{4}'), '{5}') """. \
        format(invoice_id, total, vendor, invoice_type, status, location)
    print(query)
    execute_insert_query(query, False)

def insert_invoice_processing_status(invoice_id, status):
    query = """
    INSERT INTO public.invoice_data_processing_status(
	 invoice_id, processing_status)
	VALUES ({0},
	(select id from public.invoice_processing_status_lk where status_type_code = '{1}'))"""\
        .format(invoice_id,status)
    print(query)
    execute_insert_query(query)

def insert_into_sales_order(insert_values):
    query = """INSERT INTO public.salesorder(
	date_added, id, reference, delivery_name, charge_description, charge, calculated, 
    rate_matched, tor_matched, description,
    source, 
    destination, 
    invoice_id, route_matched, 
    invoice_type)
    Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    (select state_id from invoice_states where state_code = %s),
    (select state_id from invoice_states where state_code = %s),
    %s,%s,
    (select id from invoice_type_lk where invoice_type_code = %s))
	"""

    execute_insert_query(query, True, insert_values)


def insert_consignments(insert_values):
    print("insert consignments")
    print(insert_values)
    query = """INSERT INTO public.consignments(
	reference, customer_name, suburb, delivery_date, carton_count, pallet_count, charge_description, base_charges,
	 fuel_charges, pallet_calculated, carton_calculated,
	 rate_matched, tor_matched, route_matched, description,
	 source, destination, 
	 invoice_id, invoice_type,
	zone)
    Values 
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    (select state_id from invoice_states where state_code = %s),
    (select state_id from invoice_states where state_code = %s),
   
    %s,(select id from invoice_type_lk where invoice_type_code = %s),
    %s)
    """
    print(query)
    execute_insert_query(query, True, insert_values)


def insert_invoice_data_auspost(insert_values):
    query = """INSERT INTO public.invoice_data_auspost(
	name, region,
	from_state, to_state, 
	from_postal_code, to_postal_code,
	amt_incl_tax,amt_excl_tax, consignment_id, article_id, 
	billing_date, billed_weight, invoice_id,calculated_amt_incl_tax, 
	calculated_amt_excl_tax, description, article_id_matched, route_matched,  invoice_type, rate_matched)
	VALUES (
	%s,%s,
	(select state_id from invoice_states where state_code = %s), (select state_id from invoice_states where state_code = %s),
	%s,%s,
	%s,%s,%s,%s,
	%s,%s,%s,%s,
	%s,%s,%s,%s,(select id from invoice_type_lk where invoice_type_code = %s),%s) 
	"""

    print(query)

    execute_insert_query(query, True, insert_values)


def insert_invoice_data_netlogix(insert_Values):
    query = """INSERT INTO public.invoice_data_netlogix(
	collection_date, trip_code, consignment_no, origin, destination,dest_zone,
	goods_description, count, weight, cube, excl_gst, 
	calculated, rate_matched, route_matched, tor_matched, invoice_id, 
    invoice_type, source_sate, dest_state, 
    is_demurraged,dest_postal_code ) 
    VALUES(
    %s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    (select id from invoice_type_lk where invoice_type_code = %s),
    (select state_id from invoice_states where state_code = %s),
    (select state_id from invoice_states where state_code = %s),
    %s,%s
    )"""
    execute_insert_query(query, True, insert_Values)


def insert_netlgix_summary_data(invoice_id, invoice_date, account, abn_number, due_date, invoice_address, netlogix_address, phone_num, payment_advice, note, bank_account_name, bsb, account_no):
    query = """INSERT INTO public.invoice_summary_netlogix(
        invoice_id, invoice_date, account, abn_number, due_date, invoice_address, netlogix_address, phone_num, payment_advice, note, bank_account_name, bsb, account_no)
        VALUES ({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}')
        """.format(invoice_id, invoice_date, account, abn_number, due_date, invoice_address, netlogix_address, phone_num, payment_advice, note, bank_account_name, bsb, account_no)

    execute_insert_query(query)



def select_invoices_received_data(invoice_type):
    query = """select t1.vendor, t1.invoice_id, t3.invoice_name as supplier, t1.total,t1.uploaded_date,
               t2.status
               from public.invoice_data t1 
               inner join  public.invoice_status_lk t2 on t1.status_id =t2.id
               inner join  public.invoice_type_lk t3 on t1.invoice_type_id = t3.id
               where t1.invoice_type_id = (select id from invoice_type_lk where invoice_type_code = '{0}')
               """.format(invoice_type)
    print("in select invoice_recieved data")
    result = execute_select_query(query)
    return result


def select_invoices_status_data(invoice_type):
    query = """select  count(status_id) as invoice_count, 
        t2.status from invoice_status_lk t2
        left join invoice_data t1 on t1.status_id = t2.id and (t1.invoice_type_id 
                       = (select id from invoice_type_lk where invoice_type_code = '{0}'))
        group by status_id, t2.status

        union 

        select count(status_id) as invoice_count,
        'Total invoices' as status from  invoice_data 
        where invoice_type_id = (select id from invoice_type_lk where invoice_type_code = '{0}')
        """.format(invoice_type)
    print("in select invoice status data")
    result = execute_select_query(query)
    return result


def select_invoice_processing_status(invoice_id):
    query = """
        select distinct ipsl.id from invoice_data_processing_status idps
        inner join invoice_processing_status_lk  ipsl on 
        idps.processing_status = ipsl.id
        where invoice_id = {0}
    """.format(invoice_id)
    result = execute_select_query(query)
    return result


def update_invoice_status(invoice_id, status):
    print(invoice_id, status)
    query = """update invoice_data set status_id = 
    (select  id  from  public.invoice_status_lk where  status_type_code   = '{0}') 
    where invoice_id = {1}""".format(status, invoice_id)
    print(query)
    result = execute_update_query(query)
    return result


def select_invoice_path(invoice_id):
    print("select path")
    query = """select  invoice_path from public.invoice_data where invoice_id = {0}""".format(invoice_id)
    result = execute_select_query(query)
    return result[0]


def select_sales_order_line_items(invoice_id):
    query = """ select  
    reference, delivery_name, charge_description, 
    charge as "total_charges", calculated, description, route_matched, tor_matched,
    CASE
    WHEN (abs(charge - calculated) <= 0.01)   and route_matched and tor_matched THEN 'MATCHED'
    ELSE 'NOT MATCHED'
    END AS status,
    concat_ws(', ', 
          case when not (abs(charge - calculated) <= 0.01) THEN 'Rate  Not Matched' end, 
          case when not tor_matched THEN ' TOR Not Found ' end, 
          case when not route_matched THEN  'Route Not Matched' end, 
          case when not rate_matched THEN 'Rate card not found' end ) as description 
          
    from public.salesorder  
    WHERE invoice_id = {0}""".format(invoice_id)
    result = execute_select_query(query)
    return result


def select_consignments_line_items(invoice_id):
    query = """SELECT suburb, reference, charge_description, carton_count, pallet_count, 
    base_charges + fuel_charges as total_charges, pallet_calculated + carton_calculated as calculated,

    CASE
    WHEN (abs( (base_charges + fuel_charges) - (pallet_calculated + carton_calculated)) <= 0.01) 
    and route_matched and tor_matched  and route_matched THEN 'MATCHED'
    ELSE 'NOT MATCHED'
    END AS status,
    concat_ws(', ', 
          case when not (abs((base_charges + fuel_charges) - (pallet_calculated + carton_calculated)) <= 0.01)  
          THEN 'Rate  Not Matched' end, 
          case when not tor_matched THEN ' TOR Not Found ' end, 
          case when not route_matched THEN  'Route Not Matched' end, 
          case when not rate_matched THEN 'Rate card not found' end ) as description 
     FROM public.consignments
     WHERE invoice_id =  {0}""".format(invoice_id)
    result = execute_select_query(query)

    print("query executed")
    return result


def select_netlogix_line_items(invoice_id):
    ### change table and details
    query = """    
    select  consignment_no, origin, dest_zone, goods_description, count, weight,cube,excl_gst,
        calculated,
    CASE 
    WHEN  is_demurraged  THEN 'DEMURRAGE' 
    WHEN ( abs(excl_gst - calculated) <= 0.01)  and route_matched and tor_matched and rate_matched THEN 'MATCHED'
    ELSE 'NOT MATCHED'
    END AS status,
    concat_ws(', ', 
          case when not (abs(excl_gst - calculated) <= 0.01) THEN 'Rate  Not Matched' end, 
          case when not tor_matched THEN ' TOR Not Found ' end, 
          case when not route_matched THEN  'Route Not Matched' end ,
          case when not rate_matched THEN 'Rate card not found' end ) as description 
    from public.invoice_data_netlogix  where invoice_id = {0}
    """.format(invoice_id)
    result = execute_select_query(query)
    return result


def select_auspost_line_items(invoice_id):
    query = """SELECT  t2.state_code,from_postal_code, t2.state_code, to_postal_code, billed_weight ,amt_incl_tax, 
    calculated_amt_incl_tax, amt_excl_tax,calculated_amt_excl_tax, description,
    CASE
    WHEN ( abs( amt_incl_tax - calculated_amt_incl_tax ) <= 0.01)  and route_matched and article_id_matched  THEN 'MATCHED'
    ELSE 'NOT MATCHED'
    END AS status,
    concat_ws(', ', 
          case when not (abs( amt_incl_tax - calculated_amt_incl_tax ) <= 0.01) THEN 'Rate  Not Matched' end, 
          case when not article_id_matched  THEN ' Article ID Not Found ' end, 
          case when not rate_matched THEN 'Rate card not found' end ) as description 
    FROM public.invoice_data_auspost t1
    inner join public.invoice_states t2 on t1.from_state = t2.state_id
    where t1.invoice_id  = {0}""".format(invoice_id)
    print("in auspost line items")
    result = execute_select_query(query)
    return result


def select_netlogix_variance_summary(invoice_id):
    query = """   
            with 
            tor_status as(
            select CASE  
            WHEN count(tor_matched) > 0 THEN  'TOR Not found' ELSE 'TOR FOUND' END AS order_ref
            from invoice_data_netlogix
            where invoice_id = {0}	and tor_matched = false
            ),
            route_status as(
            select CASE  
            WHEN count(route_matched) > 0 THEN 'Not Matched '  ELSE 'Matched' END AS delivery_route
            from invoice_data_netlogix
            where invoice_id = {0} and route_matched = false
            ),
            matched_status as( 
            select  count(*)  from public.invoice_data_netlogix 
            where is_demurraged  = false 
            and ( abs(excl_gst - calculated) <= 0.01)  
            and route_matched = true
            and tor_matched  = true 
            and rate_matched = true
            and invoice_id = {0}
                    ),
            total_count as (
            select count(*)   from invoice_data_netlogix
            where  is_demurraged = false and rate_matched= true and invoice_id = {0}
            ),
            path as (
            select  invoice_path from public.invoice_data where invoice_id = {0} 
            ),
            total as (
            select coalesce(round( sum(idn.excl_gst ),2),0) "total_cost" 
            from public.invoice_data_netlogix idn
            where idn.rate_matched = true and idn.is_demurraged = false and
            invoice_id= {0} 
            ),
            overcharged as (
            select coalesce(round(sum(idn.excl_gst) - sum(idn.calculated),2),0)  "difference"
            from public.invoice_data_netlogix idn
            where idn.rate_matched = true and idn.is_demurraged = false and (excl_gst - calculated)>0 and
            invoice_id= {0} 
            ),
            undercharged as (
            select abs(coalesce(round(sum(idn.excl_gst) - sum(idn.calculated),2),0))  "difference"
            from public.invoice_data_netlogix idn
            where idn.rate_matched = true and idn.is_demurraged = false and (idn.excl_gst - idn.calculated)<0 and
            invoice_id= {0} 
            )
            select 
            (select order_ref from  tor_status) as tor_status,
            (select delivery_route from  route_status) as delivery_route,
            (select * from matched_status) as matched_line_items,
            (select  *  from total_count ) as total_line_items,
            (select * from path) as file_path,
            (select  * from total) as total_amount,
            (select * from overcharged) as over_charged,
            (select * from undercharged) as under_charged
  
            """.format(invoice_id)
    result = execute_select_query(query)
    return result[0]


def select_chill_sales_order_variance_summary(invoice_id):
    query = """
   with 
            tor_status as(
            select CASE  
            WHEN count(tor_matched) > 0 THEN  'TOR Not found' ELSE 'TOR FOUND' END AS order_ref
            from public.salesorder
            where invoice_id = {0}	and tor_matched = false
            ),
            route_status as(
            select CASE  
            WHEN count(route_matched) > 0 THEN 'Not Matched '  ELSE 'Matched' END AS delivery_route
            from public.salesorder
            where invoice_id = {0} and route_matched = false
            ),
            matched_status as( 
            select count(*)  from public.salesorder
            where  invoice_id = {0}
            and rate_matched = true  
            and (abs( calculated - charge) <= 0.01  )
            and route_matched = true 
            ),
            total_count as (
            select count(*)   from public.salesorder
            where invoice_id = {0}
            ),
            path as (
            select  invoice_path from public.invoice_data where invoice_id = {0} 
            ),
             total as(
              select coalesce(round(sum(so.charge),2),0) "totalCost"
              from public.salesorder so
              where so.rate_matched = true and  invoice_id = {0} 
              ),
            overcharged as (
            select coalesce(round(sum(so.charge)-(sum(so.calculated)),2),0)  
            "difference" from public.salesorder so
            where so.rate_matched = true and 
            ((so.charge)-(so.calculated))>0 and
            invoice_id= {0} 
            ),
            undercharged as (
             select abs(coalesce(round(sum(so.charge)-(sum(so.calculated)),2),0))  
            "difference" from public.salesorder so
            where so.rate_matched = true and 
            ((so.charge)-(so.calculated))<0 and
            invoice_id= {0} 
            )
            select 
            (select order_ref from  tor_status) as tor_status,
            (select delivery_route from  route_status) as delivery_route,
            (select  * from matched_status) as matched_line_items,
            (select  *  from total_count ) as total_line_items,
            (select * from path) as file_path,
            (select  * from total) as total_amount,
            (select * from overcharged) as over_charged,
            (select * from undercharged) as under_charged
           """.format(invoice_id)
    result = execute_select_query(query)
    return result[0]


def selcet_chill_consignment_variance_summary(invoice_id):
    query = """
    with 
              tor_status as(
              select CASE  
              WHEN count(tor_matched) > 0 THEN  'TOR Not found' ELSE 'TOR FOUND' END AS order_ref
              from public.consignments
              where invoice_id = {0}	and tor_matched = false
              ),
              route_status as(
              select CASE  
              WHEN count(route_matched) > 0 THEN 'Not Matched '  ELSE 'Matched' END AS delivery_route
              from public.consignments
              where invoice_id = {0} and route_matched = false
              ),
              matched_status as( 
               select count(*)
               from public.consignments
               where  invoice_id = {0} 
               and tor_matched = true 
               and route_matched = true 
               and abs((base_charges + fuel_charges) - (pallet_calculated + carton_calculated)) <= 0.01
              ),
              total_count as (
              select count(*)   from public.consignments
              where invoice_id = {0}
              ),
              path as (
              select  invoice_path from public.invoice_data where invoice_id = {0} 
              ),
              total as(
              select coalesce(round(sum(con.base_charges+con.fuel_charges),2),0) "totalCost"
              from public.consignments con
              where con.rate_matched = true and  invoice_id = {0} 
              ),
            overcharged as (
            select coalesce(round(sum(con.base_charges+con.fuel_charges)-(sum(con.pallet_calculated+con.carton_calculated)),2),0)  
            "difference" from public.consignments con
            where con.rate_matched = true and 
            ((con.base_charges+con.fuel_charges)-(con.pallet_calculated+con.carton_calculated))>0 and
            invoice_id= {0} 
            ),
            undercharged as (
            select abs(coalesce(round(sum(con.base_charges+con.fuel_charges)-(sum(con.pallet_calculated+con.carton_calculated)),2),0))  
            "difference" from public.consignments con
            where con.rate_matched = true and ((con.base_charges+con.fuel_charges)-(pallet_calculated+carton_calculated))<0 and
            invoice_id= {0} 
            )
              select 
            (select order_ref from  tor_status) as tor_status,
            (select delivery_route from  route_status) as delivery_route,
            (select  * from matched_status) as matched_line_items,
            (select   *  from total_count ) as total_line_items,
            (select * from path) as file_path,
            (select  * from total) as total_amount,
            (select * from overcharged) as over_charged,
            (select * from undercharged) as under_charged
              
              
      """.format(invoice_id)
    result = execute_select_query(query)
    return result[0]


def select_auspost_variance_summary(invoice_id):
    query = """
    with 
            tor_status as(
            select CASE  
            WHEN count(article_id_matched ) > 0 THEN  'Article ID Not found' ELSE 'Article ID FOUND' END AS order_ref
            from public.invoice_data_auspost  
            where invoice_id = {0}	and article_id_matched = false
            ),
            matched_status as( 
            select count(*)
            from public.invoice_data_auspost  
            where invoice_id = {0}
            and (rate_matched   
            and  article_id_matched  
            and abs( amt_incl_tax - calculated_amt_incl_tax ) <= 0.01
            and abs(  amt_excl_tax - calculated_amt_excl_tax ) <= 0.01)
            ),
            total_count as (
            select count(*)   from public.invoice_data_auspost  
            where invoice_id = {0}
            ),
            path as (
            select  invoice_path from public.invoice_data where invoice_id = {0} 
            ),
            total as(
              select coalesce(round(sum(ida.amt_incl_tax),2),0) "total_cost" 
              from public.invoice_data_auspost ida
              where ida.rate_matched = true and  true and  invoice_id = {0} 
              ),
            overcharged as (
            select coalesce(round(sum(ida.amt_incl_tax) - sum(ida.calculated_amt_incl_tax),2),0)  "difference" 
            from public.invoice_data_auspost ida
            where ida.rate_matched = true and (ida.amt_incl_tax - ida.calculated_amt_incl_tax)>0 and
            invoice_id= {0} 
            ),
            undercharged as (
            select abs(coalesce(round(sum(ida.amt_incl_tax) - sum(ida.calculated_amt_incl_tax),2),0))  "difference" 
            from public.invoice_data_auspost ida
            where ida.rate_matched = true and (ida.amt_incl_tax - ida.calculated_amt_incl_tax)<0 and  
            invoice_id= {0} 
            )

            select 
            (select order_ref from  tor_status) as tor_status,
            (select 'Not Validated') as delivery_route,
            (select  * from matched_status) as matched_line_items,
            (select   *  from total_count ) as total_line_items,
            (select * from path) as file_path,
            (select  * from total) as total_amount,
            (select * from overcharged) as over_charged,
            (select * from undercharged) as under_charged
              
        """.format(invoice_id)
    result = execute_select_query(query)
    return result[0]


def select_invoice_summary_netlogix(invoice_id):
    query = """SELECT * FROM public.invoice_summary_netlogix
        where invoice_id = {0}
        """.format(invoice_id)
    result = execute_select_query(query)
    return result

## for graphs


async def get_invoices_processed_query():
    query = """ with invoice as ( select 
             array[ 'Netlogix', 'Auspost', 'Chill' ] "name",
             array[ (select count(*) "NETLOGIX" from public.invoice_data
             where invoice_type_id=(select id from public.invoice_type_lk where invoice_type_code='NETLOGIX' )), 
             (select count(*) "AUSPOST" from public.invoice_data
             where invoice_type_id=(select id from public.invoice_type_lk where invoice_type_code='AUSPOST' )),
             (select count(*) from public.invoice_data
             where invoice_type_id=(select id from public.invoice_type_lk where invoice_type_code='CHILL' ))] "data" )
             select  * from  invoice"""
    result = execute_select_query(query)
    return result


async def get_variances_found_query():
    query = """
   
with  total_amount as (   
           select array['Invoice Total Amount'] "name", 
           array[
           (select coalesce(round( sum(excl_gst ),2),0) "total_cost" from public.invoice_data_netlogix tdl
                        where rate_matched = true and is_demurraged = false),
           (select coalesce(round(sum(amt_incl_tax),2),0) "total_cost" from public.invoice_data_auspost tdl
                        where rate_matched = true ),
          (select coalesce(round(sum(tdl.base_charges+tdl.fuel_charges),2),0) "totalCost" from public.consignments tdl
                         where rate_matched = true )
               ] "data" ),
   overcharged as(select array['OverCharged']   "name",
             array[
            (select coalesce(round(sum(excl_gst) - sum(calculated),2),0)  "difference" from public.invoice_data_netlogix tdl
                        where rate_matched = true and is_demurraged = false and (excl_gst - calculated)>0),
                 
                 
            (select coalesce(round(sum(amt_incl_tax) - sum(calculated_amt_incl_tax),2),0)  "difference" from public.invoice_data_auspost tdl
                        where rate_matched = true and (amt_incl_tax - calculated_amt_incl_tax)>0),
                 
                 
            (select coalesce(round(sum(tdl.base_charges+tdl.fuel_charges)-(sum(tdl.pallet_calculated+tdl.carton_calculated)),2),0)  
                "difference" from public.consignments tdl
                where rate_matched = true and ((tdl.base_charges+tdl.fuel_charges)-(pallet_calculated+carton_calculated))>0)
             ]  "data" ),
    undercharged as(select array['UnderCharged']   "name",
             array[
            (select abs(coalesce(round(sum(excl_gst) - sum(calculated),2),0))  "difference" from public.invoice_data_netlogix tdl
                        where rate_matched = true and is_demurraged = false and (excl_gst - calculated)<0),
                 
                 
            (select abs(coalesce(round(sum(amt_incl_tax) - sum(calculated_amt_incl_tax),2),0))  "difference" from public.invoice_data_auspost tdl
                        where rate_matched = true and (amt_incl_tax - calculated_amt_incl_tax)<0),
                 
                 
            (select abs(coalesce(round(sum(tdl.base_charges+tdl.fuel_charges)-(sum(tdl.pallet_calculated+tdl.carton_calculated)),2),0))  
                "difference" from public.consignments tdl
                where rate_matched = true and ((tdl.base_charges+tdl.fuel_charges)-(pallet_calculated+carton_calculated))<0)
             ]  "data" )
            select * from total_amount hp 
            union all
            select * from overcharged oc
            union all
            select * from undercharged uc         
            """
    print(query)
    result = execute_select_query(query)
    return result


async def get_price_comparison_price_per_pallet():
        query = """ 
            with  highest_price as (   
            select 'Highest price' "name",array[ (                                   
            SELECT coalesce(round(MAX(idn.calculated/idn.count ),2),0) 
            HighestPrice  FROM public.invoice_data_netlogix idn where is_demurraged=false and rate_matched=true and idn.count>0),
            (SELECT  coalesce(round(MAX(cs.pallet_calculated/cs.pallet_count),2),0)
            HighestPrice FROM public.consignments cs where cs.rate_matched=true and pallet_count>0)] "data"),
            
            average_price as (select 'Average price' "name",array[ (                                   
            SELECT  coalesce(round(avg(idn.calculated/idn.count ),2),0) 
            averagePrice  FROM public.invoice_data_netlogix idn where is_demurraged=false and rate_matched=true and idn.count>0),
            (SELECT  coalesce(round(avg(cs.pallet_calculated/cs.pallet_count),2),0)
            averagePrice FROM public.consignments cs where cs.rate_matched=true and pallet_count>0)] "data") ,
            
            lowest_price as (select 'Lowest price' "name",array[ (                                   
            SELECT  coalesce(round(min(idn.calculated/idn.count ),2),0) 
            lowestPrice  FROM public.invoice_data_netlogix idn where is_demurraged=false and rate_matched=true and idn.count>0),
            (SELECT  coalesce(round(min(cs.pallet_calculated/cs.pallet_count),2),0)
            lowestPrice FROM public.consignments cs where cs.rate_matched=true and pallet_count>0)]"data") 
            select * from highest_price hp 
            union all
            select * from average_price ap
            union all
            select * from lowest_price lp"""
        query_result = execute_select_query(query)
        return query_result


async def get_total_spend_by_suppliers_query(source_state, destination_state):
    query = """    
                    with  netlogix as (   
                    select array['Netlogix'] "name",
                    array[(select coalesce(round(sum(excl_gst),2),0) "totalCost" from public.invoice_data_netlogix idn
                    join invoice_states iss on iss.state_id=idn.source_sate and iss.state_id=idn.dest_state
                    where source_sate=(select state_id from invoice_states where state_code='{0}')and
                    dest_state=(select state_id from invoice_states where state_code='{1}') and is_demurraged=false and rate_matched=true)] "data"  ),
                    auspost as (   
                    select array['Auspost'] "name", 
                    array[(select  coalesce(round(sum(amt_incl_tax),2),0) "totalCost" from public.invoice_data_auspost aus
							left join invoice_states iss on
							iss.state_id=aus.from_state and iss.state_id=aus.to_state
							where aus.from_state=(select state_id from public.invoice_states
                    where state_code='{0}') and 
                    aus.to_state=(select state_id from public.invoice_states
                    where state_code='{1}') and aus.rate_matched=true)] "data"),
                    
                    chill as (   
                    select array['Chill'] "name",
                    array[( select coalesce(round(sum(base_charges+fuel_charges),2),0) from  public.consignments tdl
                            left join invoice_states iss on
                            iss.state_id=tdl.source and iss.state_id=tdl.destination
                            where tdl.source=(select state_id from public.invoice_states
                    where state_code='{0}') and
                    tdl.destination=(select state_id from public.invoice_states
                    where state_code='{1}')and rate_matched=true)] "data" ) 
              select * from netlogix  
              union all
              select * from auspost
              union all
              select * from chill  """.format(source_state, destination_state)
    # print(query)
    result = execute_select_query(query)
    return result


async def get_chill_average_price_per_carton_query(source_state, destination_state):
    query = """ select array[case when 
              zone=''  
              then '0'
              else 
              zone
              end] "zone",
          round( (sum(carton_calculated)/sum(carton_count)),2) "cartonCalculated" from public.consignments tdl
            left join invoice_states iss on
            iss.state_id=tdl.source and iss.state_id=tdl.destination
            where tdl.source=(select state_id from public.invoice_states
            where state_code='{0}') and 
            tdl.destination=(select state_id from public.invoice_states
            where state_code='{1}') and carton_calculated>0  
            group by zone   
 """.format(source_state, destination_state)
    result = execute_select_query(query)
    return result


async def get_chill_average_price_per_pallet_query(source_state, destination_state):
    query = """select array[case when 
              zone='' and zone=''
              then '0'
              else 
              zone
              end] "zone",
               ARRAY[coalesce(round((sum(pallet_calculated)/sum(pallet_count)),2),0)] "palletCalculated"  from public.consignments tdl
                left join invoice_states iss on
                iss.state_id=tdl.source and iss.state_id=tdl.destination
                where tdl.source=(select state_id from public.invoice_states
                where state_code='{0}') and 
                tdl.destination=(select state_id from public.invoice_states
                where state_code='{1}')  and pallet_calculated > 0
                group by zone""".format(source_state, destination_state)
    result = execute_select_query(query)
    return result


async def get_chill_average_cost_per_order_query(from_date, to_date, state_from):
    query = """ select array[delivery_date] "date",
               array[(coalesce(round(avg(pallet_calculated+carton_calculated),2),0))] "data" from public.consignments co
               join invoice_states iss on 
               iss.state_id=co.source
               where source=(select state_id from public.invoice_states where state_code='{2}') and
               delivery_date>='{0}' and  
               delivery_date<='{1}'
               group by delivery_date
               order by delivery_date asc """.format(from_date, to_date, state_from)

    result = execute_select_query(query)
    return result


async def get_netlogix_average_cost_query(place_from, place_to):
    query = """select array[iss.state_name] "state",
array[(coalesce(round(avg(idn.calculated ),2),0))] "data" from  invoice_states iss
left join invoice_data_netlogix idn on
iss.state_id=idn.dest_state
group by iss.state_name""".format(place_from, place_to)
    result = execute_select_query(query)
    return result


async def get_auspost_average_costper_order(from_date, to_date, state_from):
    query = """select array[billing_date] "date",
              array[(coalesce(round(avg(calculated_amt_incl_tax),2),0))] "data" from public.invoice_data_auspost aus
              join invoice_states iss on 
              iss.state_id=aus.from_state
              where aus.from_state=(
              select state_id from public.invoice_states where state_code='{2}') and
              billing_date>='{0}' and
              billing_date<='{1}'
              group by billing_date
              order by billing_date asc
              """.format(from_date, to_date, state_from)
    result = execute_select_query(query)
    return result


