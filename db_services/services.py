from datetime import datetime

from .connection import coll
from .helpers import add_null_dates, get_date_format, get_query, get_requested_dates


async def get_aggregated_data(
    dt_from: str,
    dt_upto: str,
    group_type: str,
) -> str:
    dt_from = datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S")
    dt_upto = datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S")
    
    date_format = await get_date_format(group_type)
    requested_dates = await get_requested_dates(
        dt_from=dt_from,
        dt_upto=dt_upto,
        group_type=group_type,
    )
    query = await get_query(date_format, dt_from, dt_upto)
    
    query_data_cursor = coll.aggregate(query)
    query_data = [obj async for obj in query_data_cursor]

    all_data = await add_null_dates(requested_dates, query_data)

    dataset, labels = [], []
    for obj in all_data:
        dataset.append(obj['data'])
        labels.append(datetime.strptime(obj['_id'], date_format).isoformat())

    result = {"dataset": dataset, "labels": labels}
    return str(result)
