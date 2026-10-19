import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Event
from typing import List, Tuple, Dict, Any, Optional
from urllib.parse import urlsplit, urlunsplit

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.scraping import ScrapingChain
from app.sdk.config import settings
from app.sdk.media import MetaInfoPath
from app.db.oper.transferhistory import TransferHistoryOper
from app.sdk.media import NfoReader
from app.sdk.logging import logger
from app.sdk.network import RequestUtils
from app.plugins import _PluginBase
from app.schemas import MediaSource, MediaType
from app.sdk.media import resolve_media_identity
from app.sdk.utilities import SystemUtils

current_umask = os.umask(0)
os.umask(current_umask)

# 默认的 TMDB 图片备用域名，取自 MoviePilot 默认 SECURITY_IMAGE_DOMAINS 中的 TMDB 镜像
DEFAULT_IMAGE_DOMAINS = "image.tmdb.org\nimages.tmdb.org\nstatic-mdb.v.geilijiasu.com"


def _is_trickplay_path(path: Path) -> bool:
    """按目录路径分量排除 Jellyfin 预览缓存及其子目录，保留同名媒体文件。"""
    return any(parent.name.lower().endswith(".trickplay") for parent in path.parents) or (
        path.name.lower().endswith(".trickplay") and path.is_dir()
    )


class _ImageDownloadError(Exception):
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


class _LibraryScrapingChain(ScrapingChain):
    """
    为媒体库刮削的递归调用保留 Jellyfin 预览目录排除规则，并替换图片下载逻辑。

    主程序的图片下载只请求一次，网络抖动、TMDB 限流、代理异常或图片域名被阻断时，
    只会输出“图片下载失败”且不重试；这里改为多域名、多线路、带重试和完整性校验的下载。
    """

    # 以下参数由插件 init_plugin 写入
    retry_times: int = 3
    timeout: int = 30
    connect_timeout: int = 10
    image_domains: List[str] = DEFAULT_IMAGE_DOMAINS.split("\n")
    direct_fallback: bool = True

    # 这些状态码说明服务端暂时不可用，值得重试
    _retryable_status = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 524}

    def scrape_metadata(self, fileitem, *args, **kwargs):
        """在每次递归写入前检查目录，防止带季号的缓存被当作剧集目录。"""
        if fileitem and _is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)

    def _download_and_save_image(self, fileitem: schemas.FileItem, path: Path, url: str) -> None:
        """
        下载图片并保存到文件，依次尝试各图片域名与网络线路，每个组合失败后按退避时间重试。

        :param fileitem: 关联的媒体文件项
        :param path: 图片文件路径
        :param url: 图片下载URL
        """
        if not fileitem or not url or not path:
            return
        errors = []
        for candidate_url in self._candidate_urls(url):
            for proxies in self._candidate_proxies():
                route = "代理" if proxies else "直连"
                for attempt in range(1, self.retry_times + 1):
                    tmp_file_path = None
                    try:
                        logger.info(f"正在下载图片：{candidate_url}（{route}，第 {attempt} 次）...")
                        tmp_file_path = self._fetch_image(candidate_url, proxies=proxies, suffix=path.suffix)
                        item = self.storagechain.upload_file(
                            fileitem=fileitem, path=tmp_file_path, new_name=path.name
                        )
                        if item:
                            logger.info(f"已保存图片：{item.path}")
                        else:
                            logger.warn(f"图片保存失败：{path}")
                        return
                    except _ImageDownloadError as err:
                        errors.append(f"{candidate_url}（{route}）：{err}")
                        logger.warn(f"{candidate_url} 图片下载失败（{route}，第 {attempt} 次）：{err}")
                        if not err.retryable:
                            break
                        if attempt < self.retry_times:
                            time.sleep(err.retry_after or min(2 ** attempt, 10))
                    finally:
                        self._cleanup_temp_file(tmp_file_path)
        logger.error(f"{url} 图片下载失败，已尝试所有地址和线路：{'；'.join(errors[-3:])}")

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
        生成候选网络线路：配置了代理时先走代理，开启直连兜底时再尝试直连。
        """
        proxy = self.runtime_config.proxy
        if not proxy:
            return [None]
        return [proxy, None] if self.direct_fallback else [proxy]

    def _fetch_image(self, url: str, proxies: Optional[dict], suffix: str) -> Path:
        """
        流式下载单个图片到临时文件，并校验状态码、类型和长度。

        :param url: 图片地址
        :param proxies: 代理设置，None 为直连
        :param suffix: 临时文件后缀
        :return: 下载完成的临时文件路径，由调用方清理
        :raises _ImageDownloadError: 下载失败或内容不完整
        """
        request = RequestUtils(
            proxies=proxies,
            ua=self.runtime_config.normal_user_agent,
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
                    raise _ImageDownloadError("未获取到响应")
                if r.status_code != 200:
                    raise _ImageDownloadError(
                        f"HTTP {r.status_code}",
                        retryable=r.status_code in self._retryable_status,
                        retry_after=self._parse_retry_after(r.headers.get("Retry-After")),
                    )
                content_type = r.headers.get("Content-Type", "")
                if content_type and not content_type.lower().startswith(("image/", "application/octet-stream")):
                    raise _ImageDownloadError(f"返回内容不是图片：{content_type}")
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
                    raise _ImageDownloadError("图片内容为空")
                # 未压缩传输时可以用 Content-Length 判断连接是否中途断开
                if (expected_length and expected_length.isdigit() and not r.headers.get("Content-Encoding")
                        and received != int(expected_length)):
                    raise _ImageDownloadError(f"图片不完整：{received}/{expected_length} 字节")
            # 刮削的图片只需要读写权限
            tmp_file_path.chmod(0o666 & ~current_umask)
            return tmp_file_path
        except _ImageDownloadError:
            self._cleanup_temp_file(tmp_file_path)
            raise
        except requests.exceptions.RequestException as err:
            self._cleanup_temp_file(tmp_file_path)
            raise _ImageDownloadError(f"{type(err).__name__}：{err}")
        except Exception as err:
            self._cleanup_temp_file(tmp_file_path)
            raise _ImageDownloadError(str(err), retryable=False)

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        """解析 Retry-After 秒数，最长等待 30 秒，避免阻塞整个刮削任务。"""
        if value and value.strip().isdigit():
            return min(float(value.strip()), 30)
        return None


class IntentScraper(_PluginBase):
    """定时补齐媒体库元数据，跳过媒体服务器生成的预览缓存，并加固图片下载。"""

    # 插件名称
    plugin_name = "媒体库刮削增强"
    # 插件描述
    plugin_desc = "定时对媒体库进行刮削，补齐缺失元数据和图片，图片下载支持重试和备用域名。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "intent"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "intentscraper_"
    # 加载顺序
    plugin_order = 7
    # 可使用的用户级别
    user_level = 1

    # 私有属性
    _scheduler = None
    _scraper = None
    # 限速开关
    _enabled = False
    _onlyonce = False
    _cron = None
    _mode = ""
    _scraper_paths = ""
    _exclude_paths = ""
    # 图片下载
    _retry_times = 3
    _timeout = 30
    _image_domains = DEFAULT_IMAGE_DOMAINS
    _direct_fallback = True
    # 退出事件
    _event = Event()
    # 刮削目标类型
    _target_dir = "dir"
    _target_file = "file"

    def init_plugin(self, config: dict = None):
        """读取配置并重建一次性刮削任务。"""

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._mode = config.get("mode") or ""
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""
            self._retry_times = self.__to_int(config.get("retry_times"), 3, 1, 10)
            self._timeout = self.__to_int(config.get("timeout"), 30, 5, 300)
            self._image_domains = config.get("image_domains") or DEFAULT_IMAGE_DOMAINS
            self._direct_fallback = config.get("direct_fallback", True)

        # 图片下载参数写入刮削链
        _LibraryScrapingChain.retry_times = self._retry_times
        _LibraryScrapingChain.timeout = self._timeout
        # 兼容填写 https://域名/ 的形式
        _LibraryScrapingChain.image_domains = [
            urlsplit(d.strip() if "://" in d else f"//{d.strip()}").netloc
            for d in self._image_domains.split("\n") if d.strip()
        ]
        _LibraryScrapingChain.direct_fallback = bool(self._direct_fallback)

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self._enabled or self._onlyonce:

            if self._onlyonce:
                logger.info(f"媒体库刮削服务，立即运行一次")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(func=self.__libraryscraper, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="媒体库刮削")
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "enabled": self._enabled,
                    "cron": self._cron,
                    "mode": self._mode,
                    "scraper_paths": self._scraper_paths,
                    "exclude_paths": self._exclude_paths,
                    "retry_times": self._retry_times,
                    "timeout": self._timeout,
                    "image_domains": self._image_domains,
                    "direct_fallback": self._direct_fallback
                })
                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def get_state(self) -> bool:
        """返回定时刮削服务的启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """本插件不提供消息命令。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """本插件不提供独立 API。"""
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [{
                "id": "IntentScraper",
                "name": "媒体库刮削",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__libraryscraper,
                "kwargs": {}
            }]
        elif self._enabled:
            return [{
                "id": "IntentScraper",
                "name": "媒体库刮削",
                "trigger": CronTrigger.from_crontab("0 0 */7 * *"),
                "func": self.__libraryscraper,
                "kwargs": {}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '覆盖模式',
                                            'items': [
                                                {'title': '不覆盖已有元数据', 'value': ''},
                                                {'title': '覆盖所有元数据和图片', 'value': 'force_all'},
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'scraper_paths',
                                            'label': '削刮路径',
                                            'rows': 5,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_paths',
                                            'label': '排除路径',
                                            'rows': 2,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '刮削路径后拼接#电视剧/电影，强制指定该媒体路径媒体类型。'
                                                    '不加默认根据文件名自动识别媒体类型。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'retry_times',
                                            'label': '图片下载重试次数',
                                            'type': 'number',
                                            'placeholder': '每个地址和线路的尝试次数，默认3'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'timeout',
                                            'label': '图片下载超时（秒）',
                                            'type': 'number',
                                            'placeholder': '默认30'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'direct_fallback',
                                            'label': '代理失败时尝试直连',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'image_domains',
                                            'label': 'TMDB图片备用域名',
                                            'rows': 3,
                                            'placeholder': '每一行一个域名，按顺序尝试'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'TMDB图片下载失败时，会按顺序替换为上面的域名重试；'
                                                    '配置了代理时先走代理，失败后再直连。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 0 */7 * *",
            "mode": "",
            "scraper_paths": "",
            "exclude_paths": "",
            "retry_times": 3,
            "timeout": 30,
            "image_domains": DEFAULT_IMAGE_DOMAINS,
            "direct_fallback": True
        }

    def get_page(self) -> List[dict]:
        """本插件不提供详情页面。"""
        pass

    def __libraryscraper(self):
        """
        开始刮削媒体库
        """
        if not self._scraper_paths:
            return
        # 排除目录
        exclude_paths = self._exclude_paths.split("\n")
        # 已选择的目录
        paths = self._scraper_paths.split("\n")
        # 需要刮削的媒体目录或文件
        scraper_paths = []
        for path in paths:
            if not path:
                continue
            # 强制指定该路径媒体类型
            mtype = None
            if str(path).count("#") == 1:
                mtype = next(
                    (mediaType for mediaType in MediaType.__members__.values() if
                     mediaType.value == str(str(path).split("#")[1])),
                    None)
                path = str(path).split("#")[0]
            # 判断路径是否存在
            scraper_path = Path(path)
            if not scraper_path.exists():
                logger.warning(f"媒体库刮削路径不存在：{path}")
                continue
            logger.info(f"开始检索目录：{path} {mtype} ...")
            # 遍历所有文件
            files = SystemUtils.list_files(scraper_path, settings.RMT_MEDIAEXT)
            for file_path in files:
                if self._event.is_set():
                    logger.info(f"媒体库刮削服务停止")
                    return
                if _is_trickplay_path(file_path):
                    continue
                # 排除目录
                exclude_flag = False
                for exclude_path in exclude_paths:
                    try:
                        if file_path.is_relative_to(Path(exclude_path)):
                            exclude_flag = True
                            break
                    except Exception as err:
                        print(str(err))
                if exclude_flag:
                    logger.debug(f"{file_path} 在排除目录中，跳过 ...")
                    continue
                if mtype and not self.__match_forced_type_path(
                        file_path=file_path,
                        scraper_path=scraper_path,
                        mtype=mtype
                ):
                    logger.debug(f"{file_path} 不属于强制指定的{mtype.value}目录，跳过 ...")
                    continue
                # 识别是电影还是电视剧，强制类型只作为默认值，不污染后续文件识别结果
                file_meta = MetaInfoPath(file_path)
                file_mtype = mtype
                if not file_mtype:
                    file_mtype = file_meta.type
                    if file_mtype == MediaType.UNKNOWN:
                        file_mtype = self.__infer_type_from_path(file_path=file_path, scraper_path=scraper_path)
                scraper_item = self.__get_scrape_item(
                    file_path=file_path,
                    scraper_path=scraper_path,
                    mtype=file_mtype,
                    media_source=file_meta.media_source,
                    media_id=file_meta.media_id,
                )
                if scraper_item and not self.__contains_scrape_item(scraper_paths, scraper_item):
                    logger.info(f"发现刮削目标：{scraper_item}")
                    scraper_paths.append(scraper_item)
        # 开始刮削
        if scraper_paths:
            for item in scraper_paths:
                logger.info(f"开始刮削目标：{item[0]} ...")
                self.__scrape_path(
                    path=item[0],
                    mtype=item[1],
                    target_type=item[2],
                    media_source=item[3],
                    media_id=item[4],
                )
        else:
            logger.info(f"未发现需要刮削的目录")

    @staticmethod
    def __to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        """把表单输入转换为限定范围内的整数，非法输入使用默认值。"""
        try:
            return max(minimum, min(int(value), maximum))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def __get_scrape_item(
            file_path: Path,
            scraper_path: Path,
            mtype: MediaType,
            media_source: Optional[MediaSource] = None,
            media_id: Optional[str] = None,
    ) -> Optional[Tuple[Path, MediaType, str, Optional[MediaSource], Optional[str]]]:
        """
        根据扫描根目录和重命名格式，计算真正需要刮削的媒体目录。
        分类目录通常位于扫描根目录下方，必须用相对路径计算，否则会被误当成媒体目录。
        """
        if not file_path or not scraper_path or not mtype:
            return None

        if _is_trickplay_path(file_path):
            return None

        rename_format = settings.TV_RENAME_FORMAT if mtype == MediaType.TV else settings.MOVIE_RENAME_FORMAT
        rename_format_level = len(rename_format.strip("/").split("/")) - 1
        try:
            relative_path = file_path.relative_to(scraper_path)
        except ValueError:
            relative_path = Path(file_path.name)

        if rename_format_level >= 1:
            relative_parts = Path(relative_path).parts
            # 重命名格式中包含几层目录，就从文件往上取几层目录；前缀分类目录不会参与计算。
            if len(relative_parts) > rename_format_level:
                media_path = scraper_path.joinpath(*relative_parts[:-rename_format_level])
                return media_path, mtype, IntentScraper._target_dir, media_source, media_id

        # 扁平目录或自定义重命名格式无目录层级时，退回到单文件刮削，避免分类目录识别失败。
        return file_path, mtype, IntentScraper._target_file, media_source, media_id

    @staticmethod
    def __contains_scrape_item(
            scraper_paths: List[Tuple[Path, MediaType, str, Optional[MediaSource], Optional[str]]],
            scraper_item: Tuple[Path, MediaType, str, Optional[MediaSource], Optional[str]],
    ) -> bool:
        """
        判断刮削目标是否已存在；同一目标只刮削一次，媒体身份仅作为识别辅助信息。
        """
        return any(item[:3] == scraper_item[:3] for item in scraper_paths)

    @staticmethod
    def __match_forced_type_path(file_path: Path, scraper_path: Path, mtype: MediaType) -> bool:
        """
        强制指定媒体类型时，如果扫描根目录下同时存在“电影/电视剧”分类，则只处理匹配类型的目录。
        """
        if mtype not in (MediaType.MOVIE, MediaType.TV):
            return True
        try:
            relative_parts = file_path.relative_to(scraper_path).parts
        except ValueError:
            return True
        media_type_parts = {MediaType.MOVIE.value, MediaType.TV.value}.intersection(relative_parts)
        return not media_type_parts or mtype.value in media_type_parts

    @staticmethod
    def __infer_type_from_path(file_path: Path, scraper_path: Path) -> MediaType:
        """
        文件名无法识别类型时，从扫描根目录下的“电影/电视剧”分类层推断媒体类型。
        """
        try:
            relative_parts = file_path.relative_to(scraper_path).parts
        except ValueError:
            relative_parts = file_path.parts
        if MediaType.TV.value in relative_parts:
            return MediaType.TV
        if MediaType.MOVIE.value in relative_parts:
            return MediaType.MOVIE
        return MediaType.UNKNOWN

    def __scrape_path(
            self,
            path: Path,
            mtype: MediaType,
            target_type: str = _target_dir,
            media_source: Optional[MediaSource] = None,
            media_id: Optional[str] = None,
    ):
        """
        刮削一个媒体目录或媒体文件
        """
        if _is_trickplay_path(path):
            return

        media_source, media_id = resolve_media_identity(
            media_source=media_source,
            media_id=media_id,
        )
        media_source, media_id = self.__normalize_media_identity(media_source, media_id)
        # 优先读取本地 NFO 文件；NFO 无合法身份时保留文件路径中的统一身份。
        nfo_candidates = []
        if target_type == self._target_file:
            nfo_candidates.append(path.with_suffix(".nfo"))
        elif mtype == MediaType.MOVIE:
            nfo_candidates.extend((path / "movie.nfo", path / (path.stem + ".nfo")))
        else:
            nfo_candidates.append(path / "tvshow.nfo")
        for nfo_path in nfo_candidates:
            if not nfo_path.exists():
                continue
            nfo_source, nfo_media_id = self.__get_media_identity_from_nfo(nfo_path)
            if nfo_source:
                media_source, media_id = nfo_source, nfo_media_id
                break
        if media_source and media_id:
            logger.info(f"读取到本地 NFO 媒体身份：{media_source.value}:{media_id}")
            mediainfo = self.chain.recognize_media(
                media_source=media_source,
                media_id=str(media_id),
                mtype=mtype,
            )
        else:
            # 按名称识别
            meta = MetaInfoPath(path)
            meta.type = mtype
            mediainfo = self.chain.recognize_media(meta=meta)
        if not mediainfo:
            if target_type == self._target_dir:
                # 目录名无法识别时，通常是分类目录，继续尝试其中的具体媒体文件。
                self.__scrape_child_files(path=path, mtype=mtype)
                return
            logger.warn(f"未识别到媒体信息：{path}")
            return

        # 不跟随远端标题时，按统一媒体身份找回整理历史中的标题。
        if not settings.SCRAP_FOLLOW_TMDB:
            transfer_history = TransferHistoryOper().get_by_media_identity(
                media_source=mediainfo.media_source,
                media_id=mediainfo.media_id,
                mtype=mediainfo.type.value,
            )
            if transfer_history:
                mediainfo.title = transfer_history.title
        # 获取图片
        self.chain.obtain_images(mediainfo)
        # 刮削
        item_path = str(path).replace("\\", "/")
        if target_type == self._target_dir:
            item_path = f"{item_path}/"
        _LibraryScrapingChain().scrape_metadata(
            fileitem=schemas.FileItem(
                storage="local",
                type=target_type,
                path=item_path,
                name=path.name,
                basename=path.stem,
                extension=path.suffix[1:] if target_type == self._target_file else None,
                modify_time=path.stat().st_mtime,
            ),
            mediainfo=mediainfo,
            overwrite=True if self._mode else False
        )
        logger.info(f"{path} 刮削完成")

    def __scrape_child_files(self, path: Path, mtype: MediaType):
        """
        分类目录无法作为单个媒体识别时，继续按目录内的媒体文件逐个刮削。
        """
        child_files = SystemUtils.list_files(path, settings.RMT_MEDIAEXT)
        if not child_files:
            logger.warn(f"未识别到媒体信息：{path}")
            return
        logger.info(f"{path} 可能是分类目录，开始刮削目录内媒体文件 ...")
        for child_file in child_files:
            if self._event.is_set():
                logger.info(f"媒体库刮削服务停止")
                return
            if _is_trickplay_path(child_file):
                continue
            child_mtype = mtype
            child_meta = MetaInfoPath(child_file)
            if not child_mtype:
                child_mtype = child_meta.type
            self.__scrape_path(
                path=child_file,
                mtype=child_mtype,
                target_type=self._target_file,
                media_source=child_meta.media_source,
                media_id=child_meta.media_id,
            )

    @staticmethod
    def __get_media_identity_from_nfo(file_path: Path) -> Tuple[Optional[MediaSource], Optional[str]]:
        """
        从 NFO 中读取第一个可识别的固定来源媒体身份。

        :param file_path: NFO 文件路径
        :return: 媒体来源枚举与数据源原生 ID
        """
        if not file_path:
            return None, None
        source_xpaths = {
            MediaSource.TMDB: (
                "uniqueid[@type='Tmdb']",
                "uniqueid[@type='tmdb']",
                "uniqueid[@type='TMDB']",
                "tmdbid",
            ),
            MediaSource.IMDb: (
                "uniqueid[@type='Imdb']",
                "uniqueid[@type='imdb']",
                "uniqueid[@type='IMDB']",
                "imdbid",
            ),
            MediaSource.TVDB: (
                "uniqueid[@type='Tvdb']",
                "uniqueid[@type='tvdb']",
                "uniqueid[@type='TVDB']",
                "tvdbid",
            ),
        }
        try:
            reader = NfoReader(file_path)
            for source, xpaths in source_xpaths.items():
                for xpath in xpaths:
                    media_source, media_id = resolve_media_identity(
                        media_source=source,
                        media_id=reader.get_element_value(xpath),
                    )
                    media_source, media_id = IntentScraper.__normalize_media_identity(
                        media_source,
                        media_id,
                    )
                    if media_source:
                        return media_source, media_id
        except Exception as err:
            logger.warn(f"从 NFO 文件中获取媒体身份失败：{str(err)}")
        return None, None

    @staticmethod
    def __normalize_media_identity(
            media_source: Optional[MediaSource],
            media_id: Optional[str],
    ) -> Tuple[Optional[MediaSource], Optional[str]]:
        """过滤 NFO 中的空值、占位文本和非法 TMDB 媒体 ID。"""
        if not media_source or not media_id:
            return None, None
        normalized_id = str(media_id).strip()
        if normalized_id.casefold() in {"none", "null", "nil", "undefined", "nan"}:
            return None, None
        if media_source == MediaSource.TMDB:
            try:
                if int(normalized_id) <= 0:
                    return None, None
            except ValueError:
                return None, None
        return media_source, normalized_id

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))
