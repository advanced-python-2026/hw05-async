"""Асинхронный пайплайн обработки данных."""

from collections.abc import AsyncIterator
from pathlib import Path

# Конфигурация вариантов: формат входных данных
VARIANT_SOURCES = {
    0: "server_log",  # [2024-01-15 10:23:45] ERROR: Connection timeout
    1: "json_events",  # {"event": "click", "ts": 1705312345, "user_id": "abc"}
    2: "csv_metrics",  # 2024-01-15T10:23:45,cpu=78.5,mem=4096,disk=85.2
}


async def read_chunks(path: str | Path, chunk_size: int = 8192) -> AsyncIterator[bytes]:
    """Читает файл асинхронно чанками заданного размера.

    Параметры:
        path: путь к файлу
        chunk_size: размер чанка в байтах

    Yields:
        bytes — очередной чанк данных
    """
    # TODO: реализуй асинхронное чтение файла чанками (aiofiles)
    raise NotImplementedError("Реализуй read_chunks")
    yield  # noqa: RET503 — нужен для типизации AsyncIterator


async def parse_lines(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """Собирает строки из потока байтовых чанков.

    Чанк может содержать неполную строку — нужно буферизовать остаток
    и склеивать с началом следующего чанка.

    Параметры:
        chunks: асинхронный итератор байтовых чанков

    Yields:
        str — очередная полная строка (без \\n)
    """
    # TODO: реализуй сборку строк из чанков
    raise NotImplementedError("Реализуй parse_lines")
    yield  # noqa: RET503


async def filter_lines(lines: AsyncIterator[str], pattern: str) -> AsyncIterator[str]:
    """Фильтрует строки по подстроке или регулярному выражению.

    Параметры:
        lines: асинхронный итератор строк
        pattern: подстрока для поиска

    Yields:
        str — строки, содержащие pattern
    """
    # TODO: реализуй фильтрацию строк
    raise NotImplementedError("Реализуй filter_lines")
    yield  # noqa: RET503


async def batch(items: AsyncIterator[str], size: int) -> AsyncIterator[list[str]]:
    """Группирует элементы в батчи заданного размера.

    Последний батч может быть меньше size.

    Параметры:
        items: асинхронный итератор элементов
        size: размер батча

    Yields:
        list[str] — очередной батч элементов
    """
    # TODO: реализуй группировку в батчи
    raise NotImplementedError("Реализуй batch")
    yield  # noqa: RET503
