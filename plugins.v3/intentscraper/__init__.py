from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import List, Tuple, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.sdk.config import settings
from app.sdk.events import eventmanager, Event as PluginEvent
from app.sdk.media import MetaInfoPath
from app.db.oper.transferhistory import TransferHistoryOper
from app.sdk.media import NfoReader
from app.sdk.logging import logger
from app.sdk.services import MediaServerHelper
from app.plugins import _PluginBase
from app.schemas import MediaSource, MediaType
from app.schemas.types import EventType
from app.sdk.media import resolve_media_identity
from app.sdk.utilities import SystemUtils

from .library import SCRAPE_ITEMS, LibraryScrapingChain, build_policies, is_season_folder, is_trickplay_path
from .mediaserver import MediaServerPersonScraper, RealtimeGuard
from .network import (
    DEFAULT_IMAGE_DOMAINS, ImageDownloader, check_proxy, parse_domains, parse_proxy, patch_fanart_proxy,
    patch_tmdb_proxy, use_proxy
)
from .nfo import ACTOR_THUMB_LOCAL, ACTOR_THUMB_URL, NfoActorWriter, parse_path_mapping
from .person import PERSON_TYPE_ALL, PERSON_TYPE_NAME, PERSON_TYPE_ROLE, PersonResolver


class IntentScraper(_PluginBase):
    """定时补齐媒体库元数据和演职人员中文信息，跳过媒体服务器生成的预览缓存，并加固图片下载。"""

    # 插件名称
    plugin_name = "媒体库刮削增强"
    # 插件描述
    plugin_desc = "定时对媒体库进行刮削，补齐电影、剧集、季和集的NFO与全部图片，演员中文化并保存头像，支持独立代理、图片下载重试和备用域名。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "1.3.1"
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
    # 刮削内容
    _scrape_items: List[str] = list(SCRAPE_ITEMS)
    # NFO 演职人员
    _nfo_actor = True
    _actor_thumb = ACTOR_THUMB_LOCAL
    _actor_path_mapping = ""
    # 演职人员刮削
    _person_enabled = False
    _person_realtime = False
    _person_type = PERSON_TYPE_ALL
    _person_remove_nozh = False
    _mediaservers: List[str] = []
    _delay = 30
    # 图片下载
    _retry_times = 3
    _timeout = 30
    _image_domains = DEFAULT_IMAGE_DOMAINS
    _direct_fallback = True
    _downloader = ImageDownloader()
    # 插件代理，留空使用系统代理
    _proxy = ""
    # 退出事件
    _event = Event()
    # 实时演职人员刮削防重
    _realtime_guard = RealtimeGuard()
    # 覆盖模式
    _mode_force_all = "force_all"
    _mode_force_nfo = "force_nfo"
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
            scrape_items = config.get("scrape_items")
            self._scrape_items = list(SCRAPE_ITEMS) if scrape_items is None else list(scrape_items)
            self._nfo_actor = config.get("nfo_actor", True)
            self._actor_thumb = config.get("actor_thumb") or ACTOR_THUMB_LOCAL
            self._actor_path_mapping = config.get("actor_path_mapping") or ""
            self._person_enabled = config.get("person_enabled") or False
            self._person_realtime = config.get("person_realtime") or False
            self._person_type = config.get("person_type") or PERSON_TYPE_ALL
            self._person_remove_nozh = config.get("person_remove_nozh") or False
            self._mediaservers = config.get("mediaservers") or []
            self._delay = self.__to_int(config.get("delay"), 30, 0, 3600)
            self._retry_times = self.__to_int(config.get("retry_times"), 3, 1, 10)
            self._timeout = self.__to_int(config.get("timeout"), 30, 5, 300)
            self._image_domains = config.get("image_domains") or DEFAULT_IMAGE_DOMAINS
            self._direct_fallback = config.get("direct_fallback", True)
            self._proxy = (config.get("proxy") or "").strip()

        if self._proxy:
            patch_tmdb_proxy()
            patch_fanart_proxy()

        # 刮削内容由插件控制，不再依赖系统的刮削开关
        LibraryScrapingChain.policies = build_policies(self._scrape_items,
                                                       nfo_overwrite=self._mode == self._mode_force_nfo)

        # 媒体库图片和演职人员头像共用同一个下载器
        self._downloader = ImageDownloader(
            retry_times=self._retry_times,
            timeout=self._timeout,
            image_domains=parse_domains(self._image_domains),
            direct_fallback=bool(self._direct_fallback),
        )
        LibraryScrapingChain.downloader = self._downloader

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self._enabled or self._onlyonce:

            if self._onlyonce:
                logger.info(f"媒体库刮削服务，立即运行一次")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(func=self.__run_tasks, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="媒体库刮削")
                # 关闭一次性开关
                self._onlyonce = False
                self.__update_config()
                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def __update_config(self):
        """保存当前配置。"""
        self.update_config({
            "onlyonce": self._onlyonce,
            "enabled": self._enabled,
            "cron": self._cron,
            "mode": self._mode,
            "scraper_paths": self._scraper_paths,
            "exclude_paths": self._exclude_paths,
            "scrape_items": self._scrape_items,
            "nfo_actor": self._nfo_actor,
            "actor_thumb": self._actor_thumb,
            "actor_path_mapping": self._actor_path_mapping,
            "person_enabled": self._person_enabled,
            "person_realtime": self._person_realtime,
            "person_type": self._person_type,
            "person_remove_nozh": self._person_remove_nozh,
            "mediaservers": self._mediaservers,
            "delay": self._delay,
            "retry_times": self._retry_times,
            "timeout": self._timeout,
            "image_domains": self._image_domains,
            "direct_fallback": self._direct_fallback,
            "proxy": self._proxy
        })

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
                "func": self.__run_tasks,
                "kwargs": {}
            }]
        elif self._enabled:
            return [{
                "id": "IntentScraper",
                "name": "媒体库刮削",
                "trigger": CronTrigger.from_crontab("0 0 */7 * *"),
                "func": self.__run_tasks,
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
                                                {'title': '覆盖NFO，图片仅补缺', 'value': 'force_nfo'},
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
                                    'md': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'model': 'scrape_items',
                                            'label': '刮削内容',
                                            'items': [
                                                {'title': 'NFO', 'value': 'nfo'},
                                                {'title': '海报', 'value': 'poster'},
                                                {'title': '背景图', 'value': 'backdrop'},
                                                {'title': 'Logo', 'value': 'logo'},
                                                {'title': '光盘图', 'value': 'disc'},
                                                {'title': '横幅图', 'value': 'banner'},
                                                {'title': '缩略图', 'value': 'thumb'},
                                                {'title': '透明艺术图', 'value': 'clearart'},
                                                {'title': '横版缩略图', 'value': 'landscape'},
                                            ]
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'nfo_actor',
                                            'label': 'NFO演员中文化'
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
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'actor_thumb',
                                            'label': 'NFO演员头像',
                                            'items': [
                                                {'title': '下载到本地 .actors 目录', 'value': ACTOR_THUMB_LOCAL},
                                                {'title': '保留远程图片地址', 'value': ACTOR_THUMB_URL},
                                            ]
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
                                    'md': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'actor_path_mapping',
                                            'label': '演员头像路径映射',
                                            'rows': 2,
                                            'placeholder': '每一行一个映射：MoviePilot路径:媒体服务器路径，例如 /media:/data'
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
                                    'md': 12
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '电影、电视剧、季和集按所选内容刮削，不再使用系统设置中的刮削开关。'
                                                    'Logo、光盘图、横幅图、透明艺术图和季横幅/缩略图来自Fanart，需在系统设置中启用Fanart，电视剧还需要有TVDB ID。'
                                                    'NFO演员会从TMDB和豆瓣获取中文名称和饰演角色；头像下载到电影目录或剧集根目录的 .actors 中，'
                                                    '并把NFO中的头像地址改为媒体服务器能访问的路径，MoviePilot与媒体服务器的媒体目录不同时请填写路径映射。'
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'person_enabled',
                                            'label': '刮削演职人员',
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
                                            'model': 'person_realtime',
                                            'label': '入库后实时刮削演职人员',
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
                                            'model': 'person_remove_nozh',
                                            'label': '删除非中文演员',
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
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'mediaservers',
                                            'label': '媒体服务器',
                                            'items': [{"title": config.name, "value": config.name}
                                                      for config in MediaServerHelper().get_configs().values()]
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
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'person_type',
                                            'label': '演职人员刮削条件',
                                            'items': [
                                                {'title': '演员或角色非中文', 'value': PERSON_TYPE_ALL},
                                                {'title': '演员非中文', 'value': PERSON_TYPE_NAME},
                                                {'title': '角色非中文', 'value': PERSON_TYPE_ROLE},
                                            ]
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
                                            'model': 'delay',
                                            'label': '入库延迟时间（秒）',
                                            'type': 'number',
                                            'placeholder': '等待媒体服务器入库，默认30'
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
                                            'text': '演职人员刮削从TMDB和豆瓣获取中文名称、饰演角色、简介和头像，'
                                                    '写入所选媒体服务器（支持Emby、Jellyfin），按执行周期扫描全库；'
                                                    '开启实时刮削后，整理入库时也会处理对应媒体。'
                                                    '头像与媒体库图片使用相同的代理、重试和备用域名设置。'
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'proxy',
                                            'label': '代理地址',
                                            'placeholder': '例如 http://192.168.1.2:7890 或 socks5h://192.168.1.2:1080，留空使用系统代理'
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
                                            'text': '代理地址只对本插件的刮削生效（TMDB识别、元数据和图片下载），'
                                                    '留空使用系统代理，支持 http:// 和 socks5h://。'
                                                    'TMDB图片下载失败时，会按顺序替换为上面的域名重试；'
                                                    '有代理时先走代理，失败后再直连。'
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
            "scrape_items": list(SCRAPE_ITEMS),
            "nfo_actor": True,
            "actor_thumb": ACTOR_THUMB_LOCAL,
            "actor_path_mapping": "",
            "person_enabled": False,
            "person_realtime": False,
            "person_type": PERSON_TYPE_ALL,
            "person_remove_nozh": False,
            "mediaservers": [],
            "delay": 30,
            "retry_times": 3,
            "timeout": 30,
            "image_domains": DEFAULT_IMAGE_DOMAINS,
            "direct_fallback": True,
            "proxy": ""
        }

    def get_page(self) -> List[dict]:
        """本插件不提供详情页面。"""
        pass

    def __run_tasks(self):
        """
        定时任务：先刮削媒体库文件，再刮削媒体服务器演职人员，配置了插件代理时整个过程都在代理上下文中执行。
        """
        if not self._scraper_paths and not self._person_enabled:
            return
        proxies = parse_proxy(self._proxy)
        if proxies:
            if not patch_tmdb_proxy():
                logger.warn("TMDB 识别仍使用系统代理")
            if not patch_fanart_proxy():
                logger.warn("Fanart 图片仍使用系统代理")
            check_proxy(proxies)
        # 本轮人物信息在 NFO 和媒体服务器刮削间共用，同一个人只查询一次
        resolver = PersonResolver(chain=self.chain, stop_event=self._event)
        with use_proxy(proxies):
            if self._scraper_paths:
                self.__check_fanart()
                LibraryScrapingChain.nfo_writer = self.__new_nfo_writer(resolver)
                try:
                    self.__run_libraryscraper()
                finally:
                    LibraryScrapingChain.nfo_writer = None
            if self._person_enabled and not self._event.is_set():
                self.__new_person_scraper(resolver).scrape_library()

    def __check_fanart(self):
        """Logo、光盘图、横幅图、透明艺术图和季图片只能从 Fanart 获取，未启用时提前提示。"""
        fanart_items = {"logo", "disc", "banner", "clearart"}.intersection(self._scrape_items)
        if fanart_items and not getattr(settings, "FANART_ENABLE", True):
            logger.warn("系统设置中未启用 Fanart，Logo、光盘图、横幅图、透明艺术图和季横幅/缩略图将无法刮削")

    def __new_nfo_writer(self, resolver: PersonResolver) -> Optional[NfoActorWriter]:
        """创建本轮 NFO 演职人员处理器，不需要处理演员时返回 None。"""
        if "nfo" not in self._scrape_items:
            return None
        if not self._nfo_actor and self._actor_thumb != ACTOR_THUMB_LOCAL:
            return None
        return NfoActorWriter(
            storagechain=LibraryScrapingChain().storagechain,
            downloader=self._downloader,
            resolver=resolver,
            localize=bool(self._nfo_actor),
            thumb_mode=self._actor_thumb,
            path_mappings=parse_path_mapping(self._actor_path_mapping),
            overwrite_images=self._mode == self._mode_force_all,
        )

    def __new_person_scraper(self, resolver: Optional[PersonResolver] = None) -> MediaServerPersonScraper:
        """创建一轮演职人员刮削任务，人物缓存只在本轮内有效。"""
        return MediaServerPersonScraper(
            chain=self.chain,
            resolver=resolver or PersonResolver(chain=self.chain, stop_event=self._event),
            downloader=self._downloader,
            mediaservers=self._mediaservers,
            person_type=self._person_type,
            remove_nozh=self._person_remove_nozh,
            stop_event=self._event,
        )

    @eventmanager.register([EventType.MetadataScrape, EventType.TransferComplete])
    def scrape_person_rt(self, event: PluginEvent):
        """
        整理入库后实时刮削对应媒体的演职人员。
        """
        if not self._enabled or not self._person_enabled or not self._person_realtime:
            return
        if not event or not event.event_data:
            return
        event_data = event.event_data
        # 已开启元数据刮削的整理事件由 MetadataScrape 统一处理，避免 TransferComplete 逐文件重复触发
        if event.event_type == EventType.TransferComplete:
            transferinfo = event_data.get("transferinfo")
            if transferinfo and getattr(transferinfo, "need_scrape", False):
                logger.debug("整理完成事件已开启元数据刮削，等待 MetadataScrape 事件处理演职人员")
                return
        mediainfo = event_data.get("mediainfo")
        if not mediainfo:
            return
        # 等待媒体服务器扫描入库
        if self._delay and self._event.wait(self._delay):
            return
        season = getattr(event_data.get("meta"), "begin_season", None)
        with use_proxy(parse_proxy(self._proxy)):
            self.__new_person_scraper().scrape_media(mediainfo=mediainfo, season=season,
                                                     guard=self._realtime_guard)

    def __run_libraryscraper(self):
        """
        扫描刮削路径并逐个刮削
        """
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
                if is_trickplay_path(file_path):
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

        if is_trickplay_path(file_path):
            return None

        if mtype == MediaType.TV:
            tv_path = IntentScraper.__get_tv_path(file_path=file_path, scraper_path=scraper_path)
            if tv_path:
                return tv_path, mtype, IntentScraper._target_dir, media_source, media_id

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
    def __get_tv_path(file_path: Path, scraper_path: Path) -> Optional[Path]:
        """
        按季目录定位剧集根目录：集文件位于 Season 1、S01、第1季、Specials 等季目录中时，季目录的上一级就是剧集根目录。
        刮削路径直接指向剧集目录时也按目录整体刮削，才能生成 tvshow.nfo 和 season.nfo；刮削路径本身是季目录时返回该季目录。
        没有季目录时返回 None，按重命名格式计算。
        """
        try:
            relative_dirs = file_path.parent.relative_to(scraper_path).parts
        except ValueError:
            return None
        folder = file_path.parent
        # 从集文件所在目录向上查找，最多到刮削路径本身
        for _ in range(len(relative_dirs) + 1):
            if is_season_folder(folder.name):
                return folder if folder == scraper_path else folder.parent
            folder = folder.parent
        return None

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
        if is_trickplay_path(path):
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
            if is_season_folder(path.name):
                nfo_candidates.append(path.parent / "tvshow.nfo")
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
        overwrite = self._mode == self._mode_force_all
        LibraryScrapingChain().scrape_metadata(
            fileitem=self.__local_fileitem(path, target_type),
            mediainfo=mediainfo,
            overwrite=overwrite
        )
        if mediainfo.type == MediaType.TV and target_type == self._target_dir and is_season_folder(path.name):
            # 刮削路径本身是季目录时，补齐剧集根目录的 tvshow.nfo 和图片，不处理其他季
            LibraryScrapingChain().scrape_metadata(
                fileitem=self.__local_fileitem(path.parent, self._target_dir),
                mediainfo=mediainfo,
                recursive=False,
                overwrite=overwrite
            )
        logger.info(f"{path} 刮削完成")

    @classmethod
    def __local_fileitem(cls, path: Path, target_type: str) -> schemas.FileItem:
        """生成本地存储的刮削目标。"""
        item_path = str(path).replace("\\", "/")
        if target_type == cls._target_dir:
            item_path = f"{item_path}/"
        return schemas.FileItem(
            storage="local",
            type=target_type,
            path=item_path,
            name=path.name,
            basename=path.stem,
            extension=path.suffix[1:] if target_type == cls._target_file else None,
            modify_time=path.stat().st_mtime,
        )

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
            if is_trickplay_path(child_file):
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
