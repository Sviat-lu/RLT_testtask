import asyncio
import json
from aiogram import types
from aiogram.filters.command import Command

from db_services.services import get_aggregated_data
from telegram_bot.connection import bot, dp


# Обрабатываем команду "start"
@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await message.answer(f"Hi, {message.from_user.first_name}!")

# Обрабатываем все остальные сообщения
@dp.message()
async def cmd_return_data(message: types.Message) -> None:
    # Пытаемся получить интервал, тип группировки и сделать запрос 
    # на получение данных
    try:
        query = json.loads(message.text)
        dt_from = query.get("dt_from")
        dt_upto = query.get("dt_upto")
        group_type = query.get("group_type")
        answer = await get_aggregated_data(
            dt_from=dt_from, 
            dt_upto=dt_upto, 
            group_type=group_type,
        )
    # Если не получается - возвращаем сообщение о невалидном запросе.
    except:
        answer = ('Невалидный запос. Пример запроса:\n'
        '{"dt_from": "2022-09-01T00:00:00", '
        '"dt_upto": "2022-12-31T23:59:00", '
        '"group_type": "month"}')
    await message.answer(answer)


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
