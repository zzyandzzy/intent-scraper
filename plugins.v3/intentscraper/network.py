"""
插件网络层：插件代理、TMDB 代理挂载和带重试的图片下载器。

媒体库刮削、演职人员刮削（以及以后的 NFO 演职人员刮削）共用这里的代理与下载能力。
"""

import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, List, Optional
from urllib.parse import urlsplit, urlunsplit

import requests

from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.network import RequestUtils

current_umask = os.umask(0)
os.umask(current_umask)

# 默认的 TMDB 图片备用域名，取自 MoviePilot 默认 SECURITY_IMAGE_DOMAINS 中的 TMDB 镜像
DEFAULT_IMAGE_DOMAINS = "image.tmdb.org\nimages.tmdb.org\nstatic-mdb.v.geilijiasu.com"

# 插件代理只对当前刮削线程生效，不影响整理、订阅等系统任务
_proxy_context = threading.local()


def current_proxies() -> Optional[dict]:
    """返回当前线程的插件代理，未在插件刮削中时为 None。"""
    return getattr(_proxy_context, "proxies", None)


@contextmanager
def use_proxy(proxies: Optional[dict]) -> Iterator[None]:
    """在当前线程内启用插件代理，TMDB 请求复用同一会话以减少代理握手。"""
    if not proxies:
        yield
        return
    _proxy_context.proxies = proxies
    _proxy_context.session = requests.Session()
    try:
        yield
    finally:
        _proxy_context.session.close()
        _proxy_context.proxies = None
        _proxy_context.session = None


def patch_tmdb_proxy() -> bool:
    """
    让主程序 TMDB 客户端在插件刮削线程内改走插件代理。

    主程序 TMDb 客户端在创建时读取系统代理并固定在会话上，插件无法通过参数传入代理，
    因此包装其单次请求方法：只有当前线程设置了插件代理时才替换请求客户端，其余调用保持原样。

    :return: 是否已成功挂载
    """
    try:
        from app.modules.themoviedb.tmdbv3api.tmdb import TMDb
    except Exception as err:
        logger.warn(f"无法加载 TMDB 客户端，插件代理仅对图片下载生效：{err}")
        return False
    original = getattr(TMDb, "_request_once", None)
    if original is None:
        logger.warn("当前 MoviePilot 版本的 TMDB 客户端不兼容，插件代理仅对图片下载生效")
        return False
    if getattr(original, "_intentscraper_patched", False):
        return True

    def _request_once(self, method, url, data, json):
        proxies = current_proxies()
        if not proxies:
            return original(self, method, url, data, json)
        req = RequestUtils(
            ua=settings.NORMAL_USER_AGENT,
            session=_proxy_context.session,
            proxies=proxies,
        )
        if method == "GET":
            return req.get_res(url, params=data, json=json)
        return req.post_res(url, data=data, json=json)

    _request_once._intentscraper_patched = True
    TMDb._request_once = _request_once
    return True


def parse_proxy(value: Optional[str]) -> Optional[dict]:
    """把代理地址转换为 requests 代理映射，未填写协议时按 http 处理。"""
    value = (value or "").strip()
    if not value:
        return None
    if "://" not in value:
        value = f"http://{value}"
    return {"http": value, "https": value}


def check_proxy(proxies: dict) -> None:
    """通过插件代理访问 TMDB API 域名，提前在日志中给出代理是否可用的结论。"""
    url = f"https://{settings.TMDB_API_DOMAIN}/3/configuration"
    # 日志中不输出代理账号密码
    proxy_parts = urlsplit(proxies.get("https"))
    proxy_desc = f"{proxy_parts.scheme}://{proxy_parts.hostname}:{proxy_parts.port}"
    try:
        # 未带 API Key 会返回 401，只要拿到响应就说明代理链路是通的
        res = RequestUtils(proxies=proxies, timeout=20).get_res(url, raise_exception=True)
        logger.info(f"插件代理 {proxy_desc} 连通性检查成功：{url} 返回 HTTP {res.status_code}")
    except requests.exceptions.RequestException as err:
        logger.error(f"插件代理 {proxy_desc} 连通性检查失败，请检查代理地址：{type(err).__name__}：{err}")


def parse_domains(value: Optional[str]) -> List[str]:
    """解析每行一个的域名配置，兼容填写 https://域名/ 的形式。"""
    return [
        urlsplit(d.strip() if "://" in d else f"//{d.strip()}").netloc
        for d in (value or DEFAULT_IMAGE_DOMAINS).split("\n") if d.strip()
    ]


class ImageDownloadError(Exception):
    """
    单次图片下载失败。

    :param reason: 失败原因
    :param retryable: 同一地址是否值得重试，404 等确定性错误无需重试
    :param retry_after: 服务端要求的等待秒数
    """

    def __init__(self, reason: str, retryable: bool = True, retry_after: Optional[float] = None):
        super().__init__(reason)
        self.retryable = retryable
        self.retry_after = retry_after


@dataclass
class DownloadedImage:
    """
    已下载到本地临时文件的图片。

    :param path: 临时文件路径，离开 ImageDownloader.fetch 上下文后删除
    :param content_type: 图片 MIME 类型
    :param url: 实际下载成功的地址
    """

    path: Path
    content_type: str
    url: str

    def read_bytes(self) -> bytes:
        """读取图片内容。"""
        return self.path.read_bytes()


class ImageDownloader:
    """
    带重试、备用域名、代理与直连兜底和完整性校验的图片下载器。

    主程序的图片下载只请求一次，网络抖动、TMDB 限流、代理异常或图片域名被阻断时，
    只会输出“图片下载失败”且不重试；这里依次尝试各图片域名与网络线路，每个组合失败后按退避时间重试。
    """

    # 这些状态码说明服务端暂时不可用，值得重试
    _retryable_status = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 524}

    def __init__(self, retry_times: int = 3, timeout: int = 30, connect_timeout: int = 10,
                 image_domains: Optional[List[str]] = None, direct_fallback: bool = True):
        self.retry_times = retry_times
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.image_domains = image_domains if image_domains is not None else parse_domains(DEFAULT_IMAGE_DOMAINS)
        self.direct_fallback = direct_fallback

    @contextmanager
    def fetch(self, url: str, suffix: str = "") -> Iterator[Optional[DownloadedImage]]:
        """
        下载图片到临时文件，上下文结束后自动清理。

        :param url: 图片地址
        :param suffix: 临时文件后缀
        :return: 下载成功时为图片，全部尝试失败时为 None（已输出错误日志）
        """
        image = self._download(url, suffix) if url else None
        try:
            yield image
        finally:
            if image:
                _cleanup_temp_file(image.path)

    def _download(self, url: str, suffix: str) -> Optional[DownloadedImage]:
        """依次尝试各候选地址和线路，返回第一个成功的图片。"""
        errors = []
        for candidate_url in self._candidate_urls(url):
            for proxies in self._candidate_proxies():
                route = "代理" if proxies else "直连"
                for attempt in range(1, self.retry_times + 1):
                    try:
                        logger.info(f"正在下载图片：{candidate_url}（{route}，第 {attempt} 次）...")
                        return self._fetch_once(candidate_url, proxies=proxies, suffix=suffix)
                    except ImageDownloadError as err:
                        errors.append(f"{candidate_url}（{route}）：{err}")
                        logger.warn(f"{candidate_url} 图片下载失败（{route}，第 {attempt} 次）：{err}")
                        if not err.retryable:
                            break
                        if attempt < self.retry_times:
                            time.sleep(err.retry_after or min(2 ** attempt, 10))
        logger.error(f"{url} 图片下载失败，已尝试所有地址和线路：{'；'.join(errors[-3:])}")
        return None

    def _candidate_urls(self, url: str) -> List[str]:
        """
        生成候选下载地址：原地址优先，TMDB 图片再依次替换为各备用域名。
        """
        candidates = [url]
        parts = urlsplit(url)
        tmdb_domains = [d for d in self.image_domains if d]
        known_domains = set(tmdb_domains) | {"image.tmdb.org", settings.TMDB_IMAGE_DOMAIN}
        if parts.hostname in known_domains and parts.path.startswith("/t/p/"):
            for domain in [settings.TMDB_IMAGE_DOMAIN] + tmdb_domains:
                candidate = urlunsplit(parts._replace(netloc=domain))
                if candidate not in candidates:
                    candidates.append(candidate)
        return candidates

    def _candidate_proxies(self) -> List[Optional[dict]]:
        """
        生成候选网络线路：优先插件代理，其次系统代理，开启直连兜底时再尝试直连。
        """
        proxy = current_proxies() or settings.PROXY
        if not proxy:
            return [None]
        return [proxy, None] if self.direct_fallback else [proxy]

    def _fetch_once(self, url: str, proxies: Optional[dict], suffix: str) -> DownloadedImage:
        """
        流式下载单个图片到临时文件，并校验状态码、类型和长度。

        :param url: 图片地址
        :param proxies: 代理设置，None 为直连
        :param suffix: 临时文件后缀
        :return: 下载完成的图片，临时文件由调用方清理
        :raises ImageDownloadError: 下载失败或内容不完整
        """
        request = RequestUtils(
            proxies=proxies,
            ua=settings.NORMAL_USER_AGENT,
            referer="https://movie.douban.com/" if "doubanio.com" in url else None,
        )
        tmp_file_path = None
        try:
            with request.get_stream(
                    url,
                    timeout=(self.connect_timeout, self.timeout),
                    raise_exception=True,
            ) as r:
                if r is None:
                    raise ImageDownloadError("未获取到响应")
                if r.status_code != 200:
                    raise ImageDownloadError(
                        f"HTTP {r.status_code}",
                        retryable=r.status_code in self._retryable_status,
                        retry_after=self._parse_retry_after(r.headers.get("Retry-After")),
                    )
                content_type = r.headers.get("Content-Type", "")
                if content_type and not content_type.lower().startswith(("image/", "application/octet-stream")):
                    raise ImageDownloadError(f"返回内容不是图片：{content_type}")
                expected_length = r.headers.get("Content-Length")
                received = 0
                # delete_on_close 是 Python 3.12 才支持的参数，使用 delete=False 后手动清理以兼容低版本。
                with NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
                    tmp_file_path = Path(tmp_file.name)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            tmp_file.write(chunk)
                            received += len(chunk)
                    tmp_file.flush()
                if not received:
                    raise ImageDownloadError("图片内容为空")
                # 未压缩传输时可以用 Content-Length 判断连接是否中途断开
                if (expected_length and expected_length.isdigit() and not r.headers.get("Content-Encoding")
                        and received != int(expected_length)):
                    raise ImageDownloadError(f"图片不完整：{received}/{expected_length} 字节")
            # 刮削的图片只需要读写权限
            tmp_file_path.chmod(0o666 & ~current_umask)
            return DownloadedImage(
                path=tmp_file_path,
                content_type=self._image_type(content_type, tmp_file_path),
                url=url,
            )
        except ImageDownloadError:
            _cleanup_temp_file(tmp_file_path)
            raise
        except requests.exceptions.RequestException as err:
            _cleanup_temp_file(tmp_file_path)
            raise ImageDownloadError(f"{type(err).__name__}：{err}")
        except Exception as err:
            _cleanup_temp_file(tmp_file_path)
            raise ImageDownloadError(str(err), retryable=False)

    @staticmethod
    def _image_type(content_type: str, path: Path) -> str:
        """优先使用响应头中的图片类型，缺失或为通用二进制时按文件头判断。"""
        content_type = content_type.split(";")[0].strip().lower()
        if content_type.startswith("image/"):
            return content_type
        with path.open("rb") as f:
            head = f.read(12)
        if head.startswith(b"\x89PNG"):
            return "image/png"
        if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
            return "image/webp"
        if head.startswith(b"GIF8"):
            return "image/gif"
        return "image/jpeg"

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        """解析 Retry-After 秒数，最长等待 30 秒，避免阻塞整个刮削任务。"""
        if value and value.strip().isdigit():
            return min(float(value.strip()), 30)
        return None


def _cleanup_temp_file(path: Optional[Path]) -> None:
    """清理临时图片文件。"""
    if not path or not path.exists():
        return
    try:
        path.unlink()
    except OSError as err:
        logger.warn(f"临时文件清理失败：{path} - {err}")
