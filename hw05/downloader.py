"""Асинхронная загрузка файлов с ограничением параллельности."""

from pathlib import Path


async def download_all(
    urls: list[str],
    dest_dir: str | Path,
    max_concurrent: int = 5,
) -> dict[str, bool]:
    """Скачивает файлы по URL и сохраняет в dest_dir.

    Параметры:
        urls: список URL для загрузки
        dest_dir: директория для сохранения файлов
        max_concurrent: максимальное число одновременных загрузок

    Возвращает:
        dict[str, bool] — URL -> True (успех) / False (ошибка)

    Требования:
        - Использовать aiohttp для HTTP-запросов
        - Ограничить параллельность через asyncio.Semaphore
        - Обработать HTTP-ошибки и таймауты (вернуть False)
        - Показывать прогресс через tqdm
        - Имя файла — последний сегмент URL (или хеш, если нет имени)
    """
    # TODO: реализуй функцию
    raise NotImplementedError("Реализуй download_all")
