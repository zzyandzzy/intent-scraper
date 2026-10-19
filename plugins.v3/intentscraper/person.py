"""
演职人员信息解析：从 TMDB 和豆瓣获取中文名称、饰演角色、简介和头像地址。

这里只负责“查到什么”，不关心写到哪里：媒体服务器写入见 mediaserver.py，NFO 写入见 nfo.py。
"""

import random
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app import schemas
from app.chain.tmdb import TmdbChain
from app.schemas import MediaInfo
from app.schemas.types import MediaSource
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.utilities import StringUtils, convert

# 刮削条件
PERSON_TYPE_ALL = "all"
PERSON_TYPE_NAME = "name"
PERSON_TYPE_ROLE = "role"


@dataclass
class PersonRef:
    """
    待刮削的演职人员。

    :param name: 当前名称，用于匹配豆瓣演员
    :param role: 当前饰演角色
    :param tmdb_id: TMDB 人物 ID
    :param imdb_id: IMDb 人物 ID
    """

    name: str
    role: Optional[str] = None
    tmdb_id: Optional[str] = None
    imdb_id: Optional[str] = None


@dataclass
class PersonInfo:
    """
    解析到的中文演职人员信息，未获取到的字段为 None。

    :param name: 中文名称
    :param role: 中文饰演角色
    :param overview: 中文简介
    :param image_url: 头像地址，TMDB 优先，其次豆瓣
    """

    name: Optional[str] = None
    role: Optional[str] = None
    overview: Optional[str] = None
    image_url: Optional[str] = None

    @property
    def found(self) -> bool:
        """是否获取到了任意中文文本信息。"""
        return bool(self.name or self.role or self.overview)


def need_scrape(name: Optional[str], role: Optional[str], person_type: str) -> bool:
    """
    按刮削条件判断一个演职人员是否需要处理。

    :param name: 当前名称
    :param role: 当前饰演角色
    :param person_type: 刮削条件：all 名称或角色非中文、name 名称非中文、role 角色非中文
    """
    name_need = bool(name) and not StringUtils.is_chinese(name)
    role_need = bool(role) and not StringUtils.is_chinese(role)
    if person_type == PERSON_TYPE_NAME:
        return name_need
    if person_type == PERSON_TYPE_ROLE:
        return role_need
    return name_need or role_need


class PersonResolver:
    """
    演职人员中文信息解析器。

    TMDB 人物详情与豆瓣演员表按单轮任务缓存，一轮任务创建一个解析器，
    长剧集按每集处理时同一人物、同一季只会请求一次。
    """

    def __init__(self, chain: Any, stop_event: Optional[threading.Event] = None):
        """
        :param chain: 插件处理链，用于豆瓣匹配和查询
        :param stop_event: 插件停止事件，豆瓣限速等待期间可被中断
        """
        self._chain = chain
        self._stop_event = stop_event or threading.Event()
        self._person_cache: Dict[str, Optional[schemas.MediaPerson]] = {}
        self._douban_cache: Dict[Tuple, List[dict]] = {}

    def douban_actors(self, mediainfo: MediaInfo, season: Optional[int] = None) -> List[dict]:
        """
        获取媒体在豆瓣中的演员和导演列表。

        :param mediainfo: 媒体信息
        :param season: 季号，电视剧各季的演员表不同
        """
        cache_key = (mediainfo.type, mediainfo.title, mediainfo.year, season)
        if cache_key in self._douban_cache:
            return self._douban_cache[cache_key]
        # 豆瓣有访问频率限制，每次查询前随机等待 3-9 秒
        sleep_time = random.randint(3, 9)
        logger.debug(f"随机休眠 {sleep_time}秒 ...")
        if self._stop_event.wait(sleep_time):
            return []
        actors = []
        try:
            imdb_media_id = mediainfo.media_id if mediainfo.media_source == MediaSource.IMDb else None
            doubaninfo = self._chain.match_doubaninfo(name=mediainfo.title,
                                                      imdbid=imdb_media_id,
                                                      mtype=mediainfo.type,
                                                      year=mediainfo.year,
                                                      season=season)
            if doubaninfo:
                doubanitem = self._chain.douban_info(doubaninfo.get("id")) or {}
                actors = (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
            else:
                logger.debug(f"未找到豆瓣信息：{mediainfo.title_year}")
        except Exception as err:
            logger.error(f"获取 {mediainfo.title_year} 的豆瓣演员信息失败：{err}")
        self._douban_cache[cache_key] = actors
        return actors

    def resolve(self, person: PersonRef, douban_actors: Optional[Iterable[dict]] = None) -> PersonInfo:
        """
        解析一个演职人员的中文信息：TMDB 提供中文名、简介和头像，豆瓣补齐其余字段和饰演角色。

        :param person: 待刮削的演职人员
        :param douban_actors: 所属媒体的豆瓣演员列表
        """
        info = PersonInfo()
        detail = self.person_detail(person.tmdb_id)
        if detail:
            if detail.profile_path:
                info.image_url = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{detail.profile_path}"
                logger.debug(f"{person.name} 从TMDB获取到图片：{info.image_url}")
            cn_name = self.chinese_name(detail)
            if cn_name:
                logger.debug(f"{person.name} 从TMDB获取到中文名：{cn_name}")
                info.name = cn_name
                # 只有确认是同一个中文人物时才使用简介，避免用到其他语言的简介
                if detail.biography and StringUtils.is_chinese(detail.biography):
                    logger.debug(f"{person.name} 从TMDB获取到中文描述")
                    info.overview = detail.biography
        if douban_actors and not (info.name and info.overview and info.role):
            self.__fill_from_douban(person, info, douban_actors)
        return info

    def person_detail(self, tmdb_id: Optional[str]) -> Optional[schemas.MediaPerson]:
        """查询并缓存 TMDB 人物详情。"""
        if not tmdb_id:
            return None
        tmdb_id = str(tmdb_id).strip()
        if tmdb_id not in self._person_cache:
            detail = None
            try:
                detail = TmdbChain().person_detail(int(tmdb_id))
            except ValueError:
                logger.debug(f"TMDB 人物 ID 无效：{tmdb_id}")
            except Exception as err:
                logger.error(f"查询 TMDB 人物 {tmdb_id} 详情失败：{err}")
            self._person_cache[tmdb_id] = detail
        return self._person_cache[tmdb_id]

    @staticmethod
    def chinese_name(detail: schemas.MediaPerson) -> str:
        """获取 TMDB 别名中的中文名，繁体统一转为简体，便于媒体库名称匹配。"""
        try:
            for name in detail.also_known_as or []:
                if name and StringUtils.is_chinese(name):
                    return convert(name, "zh-hans")
        except Exception as err:
            logger.error(f"获取人物中文名失败：{err}")
        return ""

    @staticmethod
    def __fill_from_douban(person: PersonRef, info: PersonInfo, douban_actors: Iterable[dict]) -> None:
        """
        按名称匹配豆瓣演员，补齐中文名、简介、饰演角色和头像。

        豆瓣演员数据示例：
        {"name": "丹尼尔·克雷格", "latin_name": "Daniel Craig",
         "title": "丹尼尔·克雷格（同名）英国,英格兰,柴郡,切斯特影视演员",
         "character": "饰 詹姆斯·邦德 James Bond 007",
         "avatar": {"large": "https://qnmob3.doubanio.com/view/celebrity/raw/public/p42588.jpg?..."}}
        """
        for actor in douban_actors:
            if person.name not in (actor.get("latin_name"), actor.get("name")):
                continue
            if not info.name and actor.get("name"):
                logger.debug(f"{person.name} 从豆瓣中获取到中文名：{actor.get('name')}")
                info.name = actor.get("name")
            if not info.overview and actor.get("title"):
                logger.debug(f"{person.name} 从豆瓣中获取到中文描述：{actor.get('title')}")
                info.overview = actor.get("title")
            if not info.role and actor.get("character"):
                # "饰 詹姆斯·邦德 James Bond 007" -> "詹姆斯·邦德 James Bond 007"
                character = re.sub(r"饰\s+", "", actor.get("character"))
                character = re.sub("演员", "", character).strip()
                if character:
                    logger.debug(f"{person.name} 从豆瓣中获取到饰演角色：{character}")
                    info.role = character
            if not info.image_url:
                avatar = actor.get("avatar") or {}
                if avatar.get("large"):
                    logger.debug(f"{person.name} 从豆瓣中获取到图片：{avatar.get('large')}")
                    info.image_url = avatar.get("large")
            break
