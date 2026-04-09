"""Тесты задания 5.1 — Исправление блокирующих операций."""

import ast
import asyncio
from pathlib import Path

import pytest

# Path to the student's fixed file
FIXED_FILE = Path(__file__).resolve().parent.parent / "hw05" / "fixed_async.py"
BROKEN_FILE = Path(__file__).resolve().parent.parent / "src" / "broken_async.py"


def _get_source() -> str:
    """Читает исходный код fixed_async.py."""
    if not FIXED_FILE.exists():
        pytest.skip("hw05/fixed_async.py не найден — скопируй src/broken_async.py и исправь")
    return FIXED_FILE.read_text(encoding="utf-8")


def _parse_imports(source: str) -> set[str]:
    """Извлекает все импортированные имена из исходного кода."""
    tree = ast.parse(source)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
            for alias in node.names:
                names.add(alias.name)
    return names


def _find_calls(source: str, func_name: str) -> list[ast.Call]:
    """Находит все вызовы функции func_name в исходном коде."""
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # Simple name: func_name(...)
            if isinstance(node.func, ast.Name) and node.func.id == func_name:
                calls.append(node)
            # Attribute: module.func_name(...)
            elif isinstance(node.func, ast.Attribute) and node.func.attr == func_name:
                calls.append(node)
    return calls


class TestNoBlockingCalls:
    """Проверяет, что блокирующие вызовы удалены из исправленного кода."""

    def test_no_requests_import(self):
        """В исправленном коде не должно быть import requests."""
        source = _get_source()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "requests", (
                        "Найден 'import requests' — используй aiohttp вместо requests"
                    )
            elif isinstance(node, ast.ImportFrom) and node.module == "requests":
                pytest.fail("Найден 'from requests import ...' — используй aiohttp")

    def test_no_requests_get(self):
        """В коде не должно быть вызовов requests.get()."""
        source = _get_source()
        calls = _find_calls(source, "get")
        for call in calls:
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
                assert call.func.value.id != "requests", (
                    "Найден requests.get() — используй aiohttp.ClientSession"
                )

    def test_no_time_sleep(self):
        """В коде не должно быть time.sleep()."""
        source = _get_source()
        calls = _find_calls(source, "sleep")
        for call in calls:
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
                assert call.func.value.id != "time", (
                    "Найден time.sleep() — используй await asyncio.sleep()"
                )

    def test_no_sync_open(self):
        """В коде не должно быть синхронного open()."""
        source = _get_source()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            # Ищем with open(...) as f: ... f.write / json.dump
            if isinstance(node, ast.With):
                for item in node.items:
                    call = item.context_expr
                    if isinstance(call, ast.Call) and isinstance(call.func, ast.Name):
                        if call.func.id == "open":
                            # Проверяем, что это не AsyncWith
                            assert False, (
                                "Найден синхронный open() в with-блоке — "
                                "используй aiofiles.open() с async with"
                            )

    def test_uses_aiohttp(self):
        """Исправленный код должен использовать aiohttp."""
        source = _get_source()
        imports = _parse_imports(source)
        assert "aiohttp" in imports, "Нужен import aiohttp для асинхронных HTTP-запросов"

    def test_uses_aiofiles(self):
        """Исправленный код должен использовать aiofiles."""
        source = _get_source()
        imports = _parse_imports(source)
        assert "aiofiles" in imports, "Нужен import aiofiles для асинхронного файлового I/O"

    def test_uses_asyncio_sleep(self):
        """Исправленный код должен использовать asyncio.sleep вместо time.sleep."""
        source = _get_source()
        assert "asyncio.sleep" in source, "Ожидается asyncio.sleep() вместо time.sleep()"

    def test_compute_statistics_uses_executor(self):
        """CPU-bound вычисления должны использовать run_in_executor."""
        source = _get_source()
        # Ищем run_in_executor в исходном коде
        assert "run_in_executor" in source or "to_thread" in source, (
            "CPU-bound операция compute_statistics должна использовать "
            "loop.run_in_executor() или asyncio.to_thread()"
        )


class TestFixedCodeFunctionality:
    """Проверяет, что исправленный код сохраняет функциональность."""

    def _import_fixed(self):
        """Динамически импортирует hw05.fixed_async."""
        if not FIXED_FILE.exists():
            pytest.skip("hw05/fixed_async.py не найден")
        import importlib.util

        spec = importlib.util.spec_from_file_location("fixed_async", FIXED_FILE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_all_functions_present(self):
        """Все исходные функции должны присутствовать."""
        mod = self._import_fixed()
        expected = [
            "fetch_user_data",
            "fetch_user_posts",
            "save_to_file",
            "load_from_file",
            "compute_statistics",
            "enrich_user",
            "wait_for_service",
            "log_message",
            "process_users",
            "main",
        ]
        for name in expected:
            assert hasattr(mod, name), f"Функция {name} отсутствует в fixed_async.py"

    def test_all_functions_are_coroutines(self):
        """Все async-функции из оригинала должны остаться корутинами."""
        mod = self._import_fixed()
        async_funcs = [
            "fetch_user_data",
            "fetch_user_posts",
            "save_to_file",
            "load_from_file",
            "compute_statistics",
            "enrich_user",
            "wait_for_service",
            "log_message",
            "process_users",
            "main",
        ]
        for name in async_funcs:
            func = getattr(mod, name, None)
            if func is not None:
                assert asyncio.iscoroutinefunction(func), (
                    f"{name} должна быть async-функцией (корутиной)"
                )

    def test_has_fix_comments(self):
        """Каждое исправление должно сопровождаться комментарием."""
        source = _get_source()
        # Студент должен добавить комментарии к исправлениям
        # Ищем слова-маркеры в комментариях
        comment_markers = ["блокиру", "event loop", "blocking", "asyncio", "aiohttp", "aiofiles"]
        comments_found = 0
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                lower = stripped.lower()
                if any(marker in lower for marker in comment_markers):
                    comments_found += 1
        assert comments_found >= 3, (
            f"Найдено только {comments_found} комментариев к исправлениям — "
            "нужно объяснить каждое исправление (минимум 3)"
        )


class TestConcurrency:
    """Проверяет, что исправленный код реально работает конкурентно."""

    @pytest.mark.asyncio
    async def test_wait_for_service_is_non_blocking(self):
        """wait_for_service не должна блокировать event loop."""
        mod = self._import_fixed_safe()
        if mod is None:
            pytest.skip("hw05/fixed_async.py не найден")

        # Запускаем wait_for_service и параллельную задачу
        counter = 0

        async def increment():
            nonlocal counter
            for _ in range(5):
                await asyncio.sleep(0.05)
                counter += 1

        # Если wait_for_service не блокирует, increment успеет выполниться
        await asyncio.gather(
            mod.wait_for_service(mod.API_BASE, timeout=0.3),
            increment(),
        )
        assert counter >= 3, (
            f"Параллельная задача выполнилась только {counter}/5 раз — "
            "wait_for_service блокирует event loop"
        )

    def _import_fixed_safe(self):
        """Импорт без падения."""
        if not FIXED_FILE.exists():
            return None
        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("fixed_async", FIXED_FILE)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
        except Exception:
            return None
