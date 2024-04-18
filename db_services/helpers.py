from datetime import datetime
from dateutil.relativedelta import relativedelta


async def get_date_format(group_type: str) -> str:
    if group_type == "month":
        date_format = "%Y-%m"
    elif group_type == "day":
        date_format = "%Y-%m-%d"
    elif group_type == "hour":
        date_format = "%Y-%m-%dT%H"
    return date_format

async def get_requested_dates(
    dt_from: datetime,
    dt_upto: datetime,
    group_type: str,
) -> list:
    requested_dates = []
    date_format = await get_date_format(group_type)
    date_unit = await get_date_unit(group_type)
    while dt_from <= dt_upto:
        requested_dates.append(dt_from.strftime(date_format))
        dt_from = dt_from + date_unit
    return requested_dates

async def get_date_unit(group_type: str) -> relativedelta:
    if group_type == "month":
        date_unit = relativedelta(months=1)
    elif group_type == "day":
        date_unit = relativedelta(days=1)
    elif group_type == "hour":
        date_unit = relativedelta(hours=1)
    return date_unit

async def add_null_dates(
    requested_dates: list,
    query_data: list,
) -> list:
    null_dates = requested_dates.copy()

    for i in range(len(query_data)):
        if query_data[i]["_id"] in requested_dates:
            null_dates.remove(query_data[i]["_id"])

    for new_date in null_dates:
        new_obj = {'_id': new_date, 'data': 0}
        query_data.insert(requested_dates.index(new_date), new_obj)

    return query_data

async def get_query(
    date_format: str,
    requested_dates: list,
) -> list:
    query = [
    {
        '$group': {
            '_id': { '$dateToString': { 'format': date_format, 'date': '$dt' } },
            'data': { '$sum': '$value' }
        }
    },
    {
        '$match': {
            '_id': { '$in': requested_dates }
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
]
    return query
    
