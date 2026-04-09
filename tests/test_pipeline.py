"""Тесты задания 5.3 — Асинхронный пайплайн с генераторами."""

import asyncio
import inspect
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from hw05.pipeline import batch, filter_lines, parse_lines, read_chunks

# ---------------------------------------------------------------------------
# Variant-specific test data
# ---------------------------------------------------------------------------

SAMPLE_DATA = {
    0: (  # server_log
        "[2024-01-15 10:23:45] ERROR: Connection timeout\n"
        "[2024-01-15 10:23:46] INFO: Retry attempt 1\n"
        "[2024-01-15 10:23:47] ERROR: Connection refused\n"
        "[2024-01-15 10:23:48] INFO: Service recovered\n"
        "[2024-01-15 10:23:49] WARNING: High latency detected\n"
        "[2024-01-15 10:23:50] ERROR: Disk space low\n"
        "[2024-01-15 10:23:51] DEBUG: Cache hit ratio 0.85\n"
        "[2024-01-15 10:23:52] INFO: Request processed\n"
    ),
    1: (  # json_events
        '{"event": "click", "ts": 1705312345, "user_id": "abc"}\n'
        '{"event": "view", "ts": 1705312346, "user_id": "def"}\n'
        '{"event": "click", "ts": 1705312347, "user_id": "abc"}\n'
        '{"event": "purchase", "ts": 1705312348, "user_id": "ghi"}\n'
        '{"event": "click", "ts": 1705312349, "user_id": "jkl"}\n'
        '{"event": "view", "ts": 1705312350, "user_id": "abc"}\n'
        '{"event": "logout", "ts": 1705312351, "user_id": "def"}\n'
        '{"event": "click", "ts": 1705312352, "user_id": "mno"}\n'
    ),
    2: (  # csv_metrics
        "2024-01-15T10:23:45,cpu=78.5,mem=4096,disk=85.2\n"
        "2024-01-15T10:23:46,cpu=82.1,mem=4200,disk=85.3\n"
        "2024-01-15T10:23:47,cpu=45.0,mem=3800,disk=85.2\n"
        "2024-01-15T10:23:48,cpu=91.3,mem=4500,disk=86.0\n"
        "2024-01-15T10:23:49,cpu=67.2,mem=4100,disk=85.5\n"
        "2024-01-15T10:23:50,cpu=88.0,mem=4300,disk=87.1\n"
        "2024-01-15T10:23:51,cpu=55.5,mem=3900,disk=85.8\n"
        "2024-01-15T10:23:52,cpu=73.0,mem=4000,disk=86.5\n"
    ),
}

FILTER_PATTERNS = {
    0: "ERROR",
    1: "click",
    2: "cpu=8",  # CPU >= 80
}

EXPECTED_FILTER_COUNTS = {
    0: 3,  # 3 ERROR lines
    1: 4,  # 4 click events
    2: 2,  # cpu=82.1, cpu=88.0 — lines containing "cpu=8"
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _collect(aiter: AsyncIterator) -> list:
    """Собирает все элементы асинхронного итератора в список."""
    result = []
    async for item in aiter:
        result.append(item)
    return result


async def _async_iter(items):
    """Создаёт AsyncIterator из обычного итерируемого."""
    for item in items:
        yield item


def _write_sample(tmp_path: Path, variant: int) -> Path:
    """Записывает тестовые данные во временный файл."""
    filepath = tmp_path / f"sample_{variant}.txt"
    filepath.write_text(SAMPLE_DATA[variant], encoding="utf-8")
    return filepath


# ---------------------------------------------------------------------------
# Тесты read_chunks
# ---------------------------------------------------------------------------


class TestReadChunks:
    """Тесты для read_chunks."""

    @pytest.mark.asyncio
    async def test_reads_file(self, tmp_path, variant):
        """read_chunks читает файл и выдаёт байты."""
        filepath = _write_sample(tmp_path, variant)
        chunks = []
        async for chunk in read_chunks(filepath):
            chunks.append(chunk)
            assert isinstance(chunk, bytes), "Чанки должны быть bytes"
        # Склеенные чанки = содержимое файла
        full = b"".join(chunks)
        assert full == SAMPLE_DATA[variant].encode("utf-8")

    @pytest.mark.asyncio
    async def test_respects_chunk_size(self, tmp_path, variant):
        """Размер чанков не превышает chunk_size."""
        filepath = _write_sample(tmp_path, variant)
        chunk_size = 16
        async for chunk in read_chunks(filepath, chunk_size=chunk_size):
            assert len(chunk) <= chunk_size, f"Чанк размером {len(chunk)} > chunk_size={chunk_size}"

    @pytest.mark.asyncio
    async def test_small_chunk_size(self, tmp_path, variant):
        """Маленький chunk_size: много чанков, но данные полные."""
        filepath = _write_sample(tmp_path, variant)
        chunks = await _collect(read_chunks(filepath, chunk_size=4))
        assert len(chunks) > 1, "При chunk_size=4 должно быть несколько чанков"
        full = b"".join(chunks)
        assert full == SAMPLE_DATA[variant].encode("utf-8")

    @pytest.mark.asyncio
    async def test_large_chunk_size(self, tmp_path, variant):
        """Большой chunk_size: весь файл в одном чанке."""
        filepath = _write_sample(tmp_path, variant)
        chunks = await _collect(read_chunks(filepath, chunk_size=1_000_000))
        assert len(chunks) == 1, "При огромном chunk_size должен быть один чанк"

    @pytest.mark.asyncio
    async def test_empty_file(self, tmp_path):
        """Пустой файл — нет чанков."""
        filepath = tmp_path / "empty.txt"
        filepath.write_text("")
        chunks = await _collect(read_chunks(filepath))
        assert chunks == []

    @pytest.mark.asyncio
    async def test_is_async_iterator(self, tmp_path, variant):
        """read_chunks возвращает AsyncIterator."""
        filepath = _write_sample(tmp_path, variant)
        result = read_chunks(filepath)
        assert hasattr(result, "__aiter__"), "read_chunks должен возвращать AsyncIterator"
        assert hasattr(result, "__anext__"), "read_chunks должен возвращать AsyncIterator"


# ---------------------------------------------------------------------------
# Тесты parse_lines
# ---------------------------------------------------------------------------


class TestParseLines:
    """Тесты для parse_lines."""

    @pytest.mark.asyncio
    async def test_assembles_lines(self, variant):
        """parse_lines собирает строки из чанков."""
        data = SAMPLE_DATA[variant].encode("utf-8")
        # Разбиваем на чанки по 20 байт (строки будут разрезаны)
        chunk_size = 20
        raw_chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        chunks = _async_iter(raw_chunks)

        lines = await _collect(parse_lines(chunks))
        expected = [line for line in SAMPLE_DATA[variant].split("\n") if line]
        assert lines == expected, f"Строки собраны неверно: {lines} != {expected}"

    @pytest.mark.asyncio
    async def test_handles_split_newline(self):
        """Строка, разрезанная между чанками, собирается корректно."""
        chunks = _async_iter([b"Hello, Wor", b"ld!\nSecond", b" line\n"])
        lines = await _collect(parse_lines(chunks))
        assert lines == ["Hello, World!", "Second line"]

    @pytest.mark.asyncio
    async def test_single_chunk_multiple_lines(self):
        """Один чанк с несколькими строками."""
        chunks = _async_iter([b"line1\nline2\nline3\n"])
        lines = await _collect(parse_lines(chunks))
        assert lines == ["line1", "line2", "line3"]

    @pytest.mark.asyncio
    async def test_no_trailing_newline(self):
        """Файл без завершающего \\n — последняя строка тоже выдаётся."""
        chunks = _async_iter([b"line1\nline2"])
        lines = await _collect(parse_lines(chunks))
        assert lines == ["line1", "line2"]

    @pytest.mark.asyncio
    async def test_empty_chunks(self):
        """Пустые чанки — нет строк."""
        chunks = _async_iter([])
        lines = await _collect(parse_lines(chunks))
        assert lines == []

    @pytest.mark.asyncio
    async def test_returns_strings(self, variant):
        """parse_lines выдаёт str, не bytes."""
        data = SAMPLE_DATA[variant].encode("utf-8")
        chunks = _async_iter([data])
        lines = await _collect(parse_lines(chunks))
        for line in lines:
            assert isinstance(line, str), f"Ожидается str, получен {type(line)}"


# ---------------------------------------------------------------------------
# Тесты filter_lines
# ---------------------------------------------------------------------------


class TestFilterLines:
    """Тесты для filter_lines."""

    @pytest.mark.asyncio
    async def test_filters_by_pattern(self, variant):
        """filter_lines оставляет только строки, содержащие pattern."""
        all_lines = [line for line in SAMPLE_DATA[variant].split("\n") if line]
        pattern = FILTER_PATTERNS[variant]

        lines = _async_iter(all_lines)
        filtered = await _collect(filter_lines(lines, pattern))

        expected = [line for line in all_lines if pattern in line]
        assert filtered == expected
        assert len(filtered) == EXPECTED_FILTER_COUNTS[variant]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        """Пустой вход — пустой выход."""
        lines = _async_iter([])
        filtered = await _collect(filter_lines(lines, "anything"))
        assert filtered == []

    @pytest.mark.asyncio
    async def test_no_matches(self, variant):
        """Паттерн, которому ничего не соответствует."""
        all_lines = [line for line in SAMPLE_DATA[variant].split("\n") if line]
        lines = _async_iter(all_lines)
        filtered = await _collect(filter_lines(lines, "ZZZZNONEXISTENT"))
        assert filtered == []

    @pytest.mark.asyncio
    async def test_all_match(self):
        """Все строки соответствуют паттерну."""
        data = ["abc1", "abc2", "abc3"]
        lines = _async_iter(data)
        filtered = await _collect(filter_lines(lines, "abc"))
        assert filtered == data


# ---------------------------------------------------------------------------
# Тесты batch
# ---------------------------------------------------------------------------


class TestBatch:
    """Тесты для batch."""

    @pytest.mark.asyncio
    async def test_groups_items(self):
        """batch группирует элементы в списки заданного размера."""
        items = _async_iter(["a", "b", "c", "d", "e"])
        batches = await _collect(batch(items, size=2))
        assert batches == [["a", "b"], ["c", "d"], ["e"]]

    @pytest.mark.asyncio
    async def test_exact_division(self):
        """Количество элементов кратно размеру батча."""
        items = _async_iter(["a", "b", "c", "d"])
        batches = await _collect(batch(items, size=2))
        assert batches == [["a", "b"], ["c", "d"]]

    @pytest.mark.asyncio
    async def test_single_batch(self):
        """Все элементы в одном батче."""
        items = _async_iter(["a", "b", "c"])
        batches = await _collect(batch(items, size=10))
        assert batches == [["a", "b", "c"]]

    @pytest.mark.asyncio
    async def test_size_one(self):
        """Размер батча = 1."""
        items = _async_iter(["a", "b", "c"])
        batches = await _collect(batch(items, size=1))
        assert batches == [["a"], ["b"], ["c"]]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        """Пустой вход — нет батчей."""
        items = _async_iter([])
        batches = await _collect(batch(items, size=3))
        assert batches == []

    @pytest.mark.asyncio
    async def test_last_batch_smaller(self, variant):
        """Последний батч может быть меньше size."""
        all_lines = [line for line in SAMPLE_DATA[variant].split("\n") if line]
        items = _async_iter(all_lines)
        batches = await _collect(batch(items, size=3))

        # Все батчи кроме последнего имеют размер 3
        for b in batches[:-1]:
            assert len(b) == 3
        # Последний — от 1 до 3
        assert 1 <= len(batches[-1]) <= 3

        # Общее количество элементов сохранено
        total = sum(len(b) for b in batches)
        assert total == len(all_lines)


# ---------------------------------------------------------------------------
# Тесты полного пайплайна
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """Интеграционные тесты: весь пайплайн от файла до батчей."""

    @pytest.mark.asyncio
    async def test_end_to_end(self, tmp_path, variant):
        """Полный пайплайн: read_chunks -> parse_lines -> filter_lines -> batch."""
        filepath = _write_sample(tmp_path, variant)
        pattern = FILTER_PATTERNS[variant]

        chunks = read_chunks(filepath, chunk_size=32)
        lines = parse_lines(chunks)
        filtered = filter_lines(lines, pattern)
        batches = await _collect(batch(filtered, size=2))

        # Проверяем количество отфильтрованных элементов
        total = sum(len(b) for b in batches)
        assert total == EXPECTED_FILTER_COUNTS[variant], (
            f"Пайплайн вернул {total} элементов, ожидалось {EXPECTED_FILTER_COUNTS[variant]}"
        )

        # Проверяем, что все элементы содержат паттерн
        for b in batches:
            for item in b:
                assert pattern in item

    @pytest.mark.asyncio
    async def test_pipeline_is_lazy(self, tmp_path, variant):
        """Пайплайн ленивый: не загружает весь файл в память.

        Проверяем, что read_chunks, parse_lines, filter_lines
        возвращают AsyncIterator, а не список.
        """
        filepath = _write_sample(tmp_path, variant)

        chunks = read_chunks(filepath, chunk_size=16)
        assert hasattr(chunks, "__aiter__"), "read_chunks должен быть ленивым (AsyncIterator)"

        lines = parse_lines(chunks)
        assert hasattr(lines, "__aiter__"), "parse_lines должен быть ленивым (AsyncIterator)"

        filtered = filter_lines(lines, FILTER_PATTERNS[variant])
        assert hasattr(filtered, "__aiter__"), "filter_lines должен быть ленивым (AsyncIterator)"

        batched = batch(filtered, size=2)
        assert hasattr(batched, "__aiter__"), "batch должен быть ленивым (AsyncIterator)"

        # Собираем, чтобы корректно завершить итераторы
        await _collect(batched)

    @pytest.mark.asyncio
    async def test_pipeline_different_chunk_sizes(self, tmp_path, variant):
        """Результат пайплайна не зависит от chunk_size."""
        filepath = _write_sample(tmp_path, variant)
        pattern = FILTER_PATTERNS[variant]

        results = []
        for cs in [4, 16, 64, 1024]:
            chunks = read_chunks(filepath, chunk_size=cs)
            lines = parse_lines(chunks)
            filtered = filter_lines(lines, pattern)
            items = await _collect(filtered)
            results.append(items)

        # Все результаты одинаковы
        for i in range(1, len(results)):
            assert results[i] == results[0], (
                f"Результат с chunk_size отличается: {results[i]} != {results[0]}"
            )


# ---------------------------------------------------------------------------
# Тесты типов
# ---------------------------------------------------------------------------


class TestTypeAnnotations:
    """Проверяет наличие правильных type annotations."""

    def test_read_chunks_is_async_generator(self):
        """read_chunks должен быть async generator function."""
        assert asyncio.iscoroutinefunction(read_chunks) or inspect.isasyncgenfunction(
            read_chunks
        ), "read_chunks должна быть async generator (async def ... yield)"

    def test_parse_lines_is_async_generator(self):
        """parse_lines должен быть async generator function."""
        assert asyncio.iscoroutinefunction(parse_lines) or inspect.isasyncgenfunction(
            parse_lines
        ), "parse_lines должна быть async generator (async def ... yield)"

    def test_filter_lines_is_async_generator(self):
        """filter_lines должен быть async generator function."""
        assert asyncio.iscoroutinefunction(filter_lines) or inspect.isasyncgenfunction(
            filter_lines
        ), "filter_lines должна быть async generator (async def ... yield)"

    def test_batch_is_async_generator(self):
        """batch должен быть async generator function."""
        assert asyncio.iscoroutinefunction(batch) or inspect.isasyncgenfunction(batch), (
            "batch должна быть async generator (async def ... yield)"
        )
