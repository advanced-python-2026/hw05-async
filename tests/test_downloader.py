"""Тесты задания 5.2 — Асинхронная загрузка с семафором."""

import asyncio

import pytest
from aiohttp import web

from hw05.downloader import download_all

# ---------------------------------------------------------------------------
# Helpers: встроенный HTTP-сервер для тестов
# ---------------------------------------------------------------------------


def _make_app(
    files: dict[str, tuple[int, bytes]] | None = None,
    delay: float = 0.0,
) -> web.Application:
    """Создаёт тестовое aiohttp-приложение.

    files: {"path": (status_code, body_bytes)}
    delay: задержка ответа в секундах (для тестов таймаута)
    """
    if files is None:
        files = {}

    async def handler(request: web.Request) -> web.Response:
        path = request.path.lstrip("/")
        if delay > 0:
            await asyncio.sleep(delay)
        if path in files:
            status, body = files[path]
            return web.Response(status=status, body=body)
        return web.Response(status=404, text="Not found")

    app = web.Application()
    app.router.add_get("/{path:.*}", handler)
    return app


@pytest.fixture
async def test_server():
    """Фикстура: запускает тестовый HTTP-сервер и возвращает (runner, base_url)."""
    files = {
        "file1.txt": (200, b"Hello, World!"),
        "file2.txt": (200, b"Second file content here"),
        "file3.bin": (200, b"\x00\x01\x02\x03" * 100),
        "large.dat": (200, b"x" * 10_000),
        "error.txt": (500, b"Internal Server Error"),
    }
    app = _make_app(files)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()

    # Получаем реальный порт
    sock = site._server.sockets[0]
    port = sock.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"

    yield base_url

    await runner.cleanup()


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


class TestDownloadAll:
    """Основные тесты download_all."""

    @pytest.mark.asyncio
    async def test_returns_dict(self, test_server, tmp_path):
        """download_all возвращает dict[str, bool]."""
        urls = [f"{test_server}/file1.txt"]
        result = await download_all(urls, str(tmp_path))
        assert isinstance(result, dict)
        for url, status in result.items():
            assert isinstance(url, str)
            assert isinstance(status, bool)

    @pytest.mark.asyncio
    async def test_downloads_single_file(self, test_server, tmp_path):
        """Загрузка одного файла: файл создан и содержимое корректно."""
        url = f"{test_server}/file1.txt"
        result = await download_all([url], str(tmp_path))

        assert result[url] is True
        downloaded = tmp_path / "file1.txt"
        assert downloaded.exists()
        assert downloaded.read_bytes() == b"Hello, World!"

    @pytest.mark.asyncio
    async def test_downloads_multiple_files(self, test_server, tmp_path):
        """Загрузка нескольких файлов."""
        urls = [
            f"{test_server}/file1.txt",
            f"{test_server}/file2.txt",
            f"{test_server}/file3.bin",
        ]
        result = await download_all(urls, str(tmp_path))

        assert all(result.values()), f"Не все файлы загружены: {result}"
        assert (tmp_path / "file1.txt").exists()
        assert (tmp_path / "file2.txt").exists()
        assert (tmp_path / "file3.bin").exists()

    @pytest.mark.asyncio
    async def test_handles_http_error(self, test_server, tmp_path):
        """HTTP 500 — возвращает False, не падает."""
        url = f"{test_server}/error.txt"
        result = await download_all([url], str(tmp_path))
        assert result[url] is False

    @pytest.mark.asyncio
    async def test_handles_404(self, test_server, tmp_path):
        """HTTP 404 — возвращает False."""
        url = f"{test_server}/nonexistent.txt"
        result = await download_all([url], str(tmp_path))
        assert result[url] is False

    @pytest.mark.asyncio
    async def test_handles_connection_error(self, tmp_path):
        """Недоступный сервер — возвращает False, не падает."""
        url = "http://127.0.0.1:1/impossible.txt"
        result = await download_all([url], str(tmp_path))
        assert result[url] is False

    @pytest.mark.asyncio
    async def test_mixed_success_and_failure(self, test_server, tmp_path):
        """Часть URL успешна, часть — нет. Все URL в результате."""
        urls = [
            f"{test_server}/file1.txt",
            f"{test_server}/error.txt",
            f"{test_server}/file2.txt",
            f"{test_server}/nonexistent.txt",
        ]
        result = await download_all(urls, str(tmp_path))

        assert len(result) == 4
        assert result[urls[0]] is True
        assert result[urls[1]] is False
        assert result[urls[2]] is True
        assert result[urls[3]] is False

    @pytest.mark.asyncio
    async def test_creates_dest_dir(self, test_server, tmp_path):
        """Создаёт dest_dir, если не существует."""
        dest = tmp_path / "subdir" / "downloads"
        url = f"{test_server}/file1.txt"
        result = await download_all([url], str(dest))
        assert result[url] is True
        assert dest.exists()

    @pytest.mark.asyncio
    async def test_empty_urls(self, tmp_path):
        """Пустой список URL — пустой результат."""
        result = await download_all([], str(tmp_path))
        assert result == {}


class TestSemaphore:
    """Проверяет ограничение параллельности."""

    @pytest.mark.asyncio
    async def test_respects_max_concurrent(self, tmp_path):
        """Одновременно не более max_concurrent загрузок."""
        max_concurrent = 2
        peak_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        # Создаём сервер, который отслеживает одновременные запросы
        async def tracking_handler(request: web.Request) -> web.Response:
            nonlocal peak_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                peak_concurrent = max(peak_concurrent, current_concurrent)
            await asyncio.sleep(0.1)  # имитация работы
            async with lock:
                current_concurrent -= 1
            return web.Response(status=200, body=b"ok")

        app = web.Application()
        app.router.add_get("/{path:.*}", tracking_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()

        sock = site._server.sockets[0]
        port = sock.getsockname()[1]
        base = f"http://127.0.0.1:{port}"

        try:
            urls = [f"{base}/file{i}.txt" for i in range(6)]
            await download_all(urls, str(tmp_path), max_concurrent=max_concurrent)

            assert peak_concurrent <= max_concurrent, (
                f"Пиковая параллельность {peak_concurrent} > max_concurrent={max_concurrent}. "
                "Семафор не ограничивает число одновременных загрузок."
            )
            assert peak_concurrent >= 1, "Должна быть хотя бы одна загрузка"
        finally:
            await runner.cleanup()

    @pytest.mark.asyncio
    async def test_default_max_concurrent(self, test_server, tmp_path):
        """По умолчанию max_concurrent=5."""
        import inspect

        sig = inspect.signature(download_all)
        default = sig.parameters["max_concurrent"].default
        assert default == 5, f"Значение по умолчанию max_concurrent={default}, ожидается 5"


class TestFileCreation:
    """Проверяет корректное сохранение файлов."""

    @pytest.mark.asyncio
    async def test_binary_content_preserved(self, test_server, tmp_path):
        """Бинарное содержимое сохраняется без изменений."""
        url = f"{test_server}/file3.bin"
        await download_all([url], str(tmp_path))
        content = (tmp_path / "file3.bin").read_bytes()
        assert content == b"\x00\x01\x02\x03" * 100

    @pytest.mark.asyncio
    async def test_large_file(self, test_server, tmp_path):
        """Большой файл загружается полностью."""
        url = f"{test_server}/large.dat"
        await download_all([url], str(tmp_path))
        content = (tmp_path / "large.dat").read_bytes()
        assert len(content) == 10_000
