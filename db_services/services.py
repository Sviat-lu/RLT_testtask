from datetime import datetime
import json

from .connection import coll
from .helpers import add_null_dates, get_date_format, get_query, get_requested_dates

# Получаем агрегированные данные
async def get_aggregated_data(
    dt_from: str,
    dt_upto: str,
    group_type: str,
) -> str:
    dt_from = datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S")
    dt_upto = datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S")
    
    # Получаем формат дат для кажого типа группировки
    date_format = await get_date_format(group_type)
    query = await get_query(
        date_format=date_format, 
        dt_from=dt_from, 
        dt_upto=dt_upto)
    
    # Делаем запрос к БД
    query_data_cursor = coll.aggregate(query)

    # Записываем всем данные из БД в список
    query_data = [obj async for obj in query_data_cursor]

    # Получаем список всех дат из указанного диапазона
    requested_dates = await get_requested_dates(
        dt_from=dt_from,
        dt_upto=dt_upto,
        group_type=group_type,
    )

    # Добавляем даты, на которые не нашлось данных в БД,
    # присваиваем им нулевые значения
    all_data = await add_null_dates(requested_dates, query_data)

    # Разбиваем данные на два списка: даты и значения
    dataset, labels = [], []
    for obj in all_data:
        dataset.append(obj['data'])
        labels.append(datetime.strptime(obj['_id'], date_format).isoformat())

    # Преобразуем данные к итоговому формату
    result = {"dataset": dataset, "labels": labels}
    return json.dumps(result)
