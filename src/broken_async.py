"""
Асинхронный сервис обработки данных.
В этом файле скрыто несколько блокирующих операций.
Найди и исправь их все.

Скопируй этот файл в hw05/fixed_async.py и внеси исправления.
Для каждого исправления добавь комментарий, объясняющий:
- Что именно блокирует event loop
- Почему это проблема
- Как исправление решает проблему
"""

import asyncio
import json
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

API_BASE = "https://jsonplaceholder.typicode.com"
DEFAULT_OUTPUT_DIR = "./output"


# ---------------------------------------------------------------------------
# Получение данных
# ---------------------------------------------------------------------------


async def fetch_user_data(user_id: int) -> dict:
    """Получает данные пользователя по API."""
    response = requests.get(f"{API_BASE}/users/{user_id}")
    response.raise_for_status()
    return response.json()


async def fetch_user_posts(user_id: int) -> list[dict]:
    """Получает посты пользователя."""
    response = requests.get(f"{API_BASE}/posts", params={"userId": user_id})
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Файловые операции
# ---------------------------------------------------------------------------


async def save_to_file(data: dict | list, filepath: str) -> None:
    """Сохраняет данные в JSON-файл."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def load_from_file(filepath: str) -> dict | list:
    """Загружает данные из JSON-файла."""
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Обработка данных
# ---------------------------------------------------------------------------


async def compute_statistics(users: list[dict]) -> dict:
    """Вычисляет статистику по пользователям.

    Имитирует тяжёлые вычисления (агрегация, нормализация).
    """
    total = 0
    for i in range(10_000_000):
        total += i * i

    names = sorted(u.get("name", "") for u in users)
    companies = {u.get("company", {}).get("name", "N/A") for u in users}

    return {
        "user_count": len(users),
        "checksum": total,
        "names": names,
        "unique_companies": sorted(companies),
    }


async def enrich_user(user: dict) -> dict:
    """Дополняет профиль пользователя его постами."""
    posts = await fetch_user_posts(user["id"])
    user["posts"] = posts
    user["post_count"] = len(posts)
    return user


# ---------------------------------------------------------------------------
# Служебные функции
# ---------------------------------------------------------------------------


async def wait_for_service(url: str, timeout: float = 2.0) -> bool:
    """Ждёт доступности внешнего сервиса."""
    time.sleep(timeout)
    return True


async def log_message(message: str, logfile: str = "service.log") -> None:
    """Записывает сообщение в лог-файл."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


# ---------------------------------------------------------------------------
# Основной пайплайн
# ---------------------------------------------------------------------------


async def process_users(user_ids: list[int], output_dir: str = DEFAULT_OUTPUT_DIR) -> list[dict]:
    """Основной пайплайн: загрузка, обогащение, сохранение."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    await log_message("Pipeline started")

    # Ждём готовности API
    await wait_for_service(API_BASE)

    # Загружаем пользователей параллельно
    tasks = [fetch_user_data(uid) for uid in user_ids]
    users = await asyncio.gather(*tasks)

    await log_message(f"Fetched {len(users)} users")

    # Обогащаем данные постами
    enriched = await asyncio.gather(*(enrich_user(u) for u in users))

    # Сохраняем каждого пользователя
    for user in enriched:
        filepath = f"{output_dir}/user_{user['id']}.json"
        await save_to_file(user, filepath)

    # Считаем статистику
    stats = await compute_statistics(enriched)
    await save_to_file(stats, f"{output_dir}/stats.json")

    await log_message("Pipeline finished")
    return enriched


async def main() -> None:
    user_ids = [1, 2, 3, 4, 5]
    users = await process_users(user_ids)
    print(f"Processed {len(users)} users")
    for user in users:
        print(f"  - {user['name']}: {user['post_count']} posts")


if __name__ == "__main__":
    asyncio.run(main())
