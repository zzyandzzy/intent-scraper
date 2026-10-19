"""
媒体服务器演职人员刮削：把解析到的中文名称、角色、简介和头像写入 Emby / Jellyfin。
"""

import base64
import copy
import json
import threading
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

from app.chain.mediaserver import MediaServerChain
from app.schemas import MediaInfo, MediaServerItem, ServiceInfo
from app.schemas.types import MediaSource, MediaType
from app.sdk.logging import logger
from app.sdk.services import MediaServerHelper
from app.sdk.utilities import StringUtils

from .network import ImageDownloader
from .person import PersonRef, PersonResolver, need_scrape

# 支持写入演职人员的媒体服务器类型
SUPPORTED_SERVER_TYPES = ("emby", "jellyfin")


class RealtimeGuard:
    """
    实时刮削防重：整理一部剧会触发多次事件，同一媒体项正在处理或刚处理完时跳过。
    """

    def __init__(self, dedup_seconds: int = 600):
        self._lock = threading.Lock()
        self._running: Set[str] = set()
        self._recent: Dict[str, float] = {}
        self._dedup_seconds = dedup_seconds

    def acquire(self, key: str, title: str = "") -> bool:
        """登记媒体项，已在处理或最近已处理时返回 False。"""
        now = time.time()
        with self._lock:
            for item_key, timestamp in list(self._recent.items()):
                if now - timestamp >= self._dedup_seconds:
                    self._recent.pop(item_key, None)
            if key in self._recent:
                logger.info(f"{title or key} 最近已触发演职人员刮削，跳过重复处理")
                return False
            if key in self._running:
                logger.info(f"{title or key} 正在执行演职人员刮削，跳过重复处理")
                return False
            self._running.add(key)
            return True

    def release(self, key: str, completed: bool = True) -> None:
        """释放媒体项，处理完成时记录防重时间。"""
        with self._lock:
            self._running.discard(key)
            if completed:
                self._recent[key] = time.time()


class MediaServerPersonScraper:
    """
    媒体服务器演职人员刮削，一轮任务（全库扫描或一次实时事件）创建一个实例。

    人物详情是媒体服务器中的全局对象，同一轮内同一人物的信息和头像只写入一次。
    """

    def __init__(self, chain: Any, resolver: PersonResolver, downloader: ImageDownloader,
                 mediaservers: List[str], person_type: str = "all", remove_nozh: bool = False,
                 stop_event: Optional[threading.Event] = None):
        """
        :param chain: 插件处理链，用于识别媒体和查询媒体服务器是否存在
        :param resolver: 演职人员信息解析器
        :param downloader: 头像下载器
        :param mediaservers: 启用的媒体服务器名称
        :param person_type: 刮削条件
        :param remove_nozh: 是否删除未获取到中文信息的演职人员
        :param stop_event: 插件停止事件
        """
        self._chain = chain
        self._resolver = resolver
        self._downloader = downloader
        self._mediaservers = mediaservers or []
        self._person_type = person_type
        self._remove_nozh = remove_nozh
        self._stop_event = stop_event or threading.Event()
        self._services: Optional[Dict[str, ServiceInfo]] = None
        self._processed_images: Set[str] = set()
        self._processed_infos: Set[str] = set()

    def services(self) -> Dict[str, ServiceInfo]:
        """获取已连接且支持演职人员写入的媒体服务器，结果在本轮内缓存。"""
        if self._services is not None:
            return self._services
        self._services = {}
        if not self._mediaservers:
            logger.warning("演职人员刮削尚未选择媒体服务器，请检查配置")
            return self._services
        services = MediaServerHelper().get_services(name_filters=self._mediaservers) or {}
        for name, service in services.items():
            if service.type not in SUPPORTED_SERVER_TYPES:
                logger.warning(f"媒体服务器 {name}（{service.type}）暂不支持演职人员刮削，已跳过")
            elif not service.instance or service.instance.is_inactive():
                logger.warning(f"媒体服务器 {name} 未连接，请检查配置")
            else:
                self._services[name] = service
        if not self._services:
            logger.warning("没有可用于演职人员刮削的媒体服务器，请检查配置")
        return self._services

    def scrape_library(self) -> None:
        """扫描所有媒体服务器的全部电影和剧集，刮削演职人员。"""
        mediaserverchain = MediaServerChain()
        for server, service in self.services().items():
            logger.info(f"开始刮削服务器 {server} 的演员信息 ...")
            for library in mediaserverchain.librarys(server):
                logger.info(f"开始刮削媒体库 {library.name} 的演员信息 ...")
                for item in mediaserverchain.items(server, library.id):
                    if self._stop_event.is_set():
                        logger.info("演职人员刮削服务停止")
                        return
                    if not item or not item.item_id or not item.item_type:
                        continue
                    if "Series" not in item.item_type and "Movie" not in item.item_type:
                        continue
                    logger.info(f"开始刮削 {item.title} 的演员信息 ...")
                    self.update_item(server=server, server_type=service.type, item=item)
                    logger.info(f"{item.title} 的演员信息刮削完成")
                logger.info(f"媒体库 {library.name} 的演员信息刮削完成")
            logger.info(f"服务器 {server} 的演员信息刮削完成")

    def scrape_media(self, mediainfo: MediaInfo, season: Optional[int] = None,
                     guard: Optional[RealtimeGuard] = None) -> None:
        """
        刮削刚入库的一个媒体在各媒体服务器中的演职人员。

        :param mediainfo: 媒体信息
        :param season: 本次入库的季号
        :param guard: 实时刮削防重
        """
        services = self.services()
        if not services:
            return
        mediaserverchain = MediaServerChain()
        title = getattr(mediainfo, "title_year", None) or getattr(mediainfo, "title", "") or "未知媒体"
        matched = False
        for server, service in services.items():
            if self._stop_event.is_set():
                return
            try:
                existsinfo = self._chain.media_exists(mediainfo=mediainfo, server=server)
            except Exception as err:
                logger.error(f"查询媒体服务器 {server} 中的 {title} 失败：{err}")
                continue
            if not existsinfo or not existsinfo.itemid:
                continue
            matched = True
            exists_server = existsinfo.server or server
            server_type = existsinfo.server_type or service.type
            item_key = f"{exists_server}:{server_type}:{existsinfo.itemid}"
            if guard and not guard.acquire(item_key, title):
                continue
            completed = False
            try:
                iteminfo = mediaserverchain.iteminfo(server=exists_server, item_id=existsinfo.itemid)
                if not iteminfo:
                    logger.warn(f"{title} 条目详情获取失败")
                    continue
                self.update_item(server=exists_server, server_type=server_type,
                                 item=iteminfo, mediainfo=mediainfo, season=season)
                completed = True
            finally:
                if guard:
                    guard.release(item_key, completed=completed)
        if not matched:
            logger.warn(f"{title} 在已配置媒体服务器中不存在")

    def update_item(self, server: str, server_type: str, item: MediaServerItem,
                    mediainfo: Optional[MediaInfo] = None, season: Optional[int] = None) -> None:
        """
        刮削一个电影或剧集的演职人员；剧集同时处理各季（Jellyfin）和各集。
        """
        if not mediainfo:
            if not item.media_source or not item.media_id:
                logger.warn(f"{item.title} 未找到媒体来源和媒体 ID，无法识别媒体信息")
                return
            mtype = MediaType.TV if item.item_type in ["Series", "show"] else MediaType.MOVIE
            try:
                mediainfo = self._chain.recognize_media(
                    mtype=mtype,
                    media_source=MediaSource(item.media_source),
                    media_id=item.media_id,
                )
            except Exception as err:
                logger.error(f"{item.title} 识别媒体信息失败：{err}")
                return
            if not mediainfo:
                logger.warn(f"{item.title} 未识别到媒体信息")
                return

        iteminfo = self.get_iteminfo(server, server_type, item.item_id)
        if not iteminfo:
            logger.warn(f"{item.title} 未找到媒体项")
            return

        if self.__need_scrape(iteminfo):
            logger.info(f"开始获取 {item.title} 的豆瓣演员信息 ...")
            douban_actors = self._resolver.douban_actors(mediainfo=mediainfo, season=season)
            self.update_peoples(server, server_type, item.item_id, iteminfo, douban_actors)
        else:
            logger.info(f"{item.title} 的人物信息已是中文，无需更新")

        if not iteminfo.get("Type") or "Series" not in iteminfo["Type"]:
            return
        seasons = self.get_items(server, server_type, parentid=item.item_id)
        if not seasons:
            logger.warn(f"{item.title} 未找到季媒体项")
            return
        for season_item in seasons.get("Items", []):
            if self._stop_event.is_set():
                return
            season_actors = None
            # Emby 的季没有人物，只有 Jellyfin 需要更新季
            if server_type == "jellyfin":
                seasoninfo = self.get_iteminfo(server, server_type, season_item.get("Id"))
                if not seasoninfo:
                    logger.warn(f"{item.title} 未找到季媒体项：{season_item.get('Id')}")
                    continue
                if self.__need_scrape(seasoninfo):
                    season_actors = self._resolver.douban_actors(mediainfo, season_item.get("IndexNumber"))
                    self.update_peoples(server, server_type, season_item.get("Id"), seasoninfo, season_actors)
                    logger.info(f"季 {seasoninfo.get('Id')} 的人物信息更新完成")
                else:
                    logger.info(f"季 {seasoninfo.get('Id')} 的人物信息已是中文，无需更新")
            episodes = self.get_items(server, server_type, parentid=season_item.get("Id"))
            if not episodes:
                logger.warn(f"{item.title} 未找到集媒体项")
                continue
            for episode in episodes.get("Items", []):
                if self._stop_event.is_set():
                    return
                episodeinfo = self.get_iteminfo(server, server_type, episode.get("Id"))
                if not episodeinfo:
                    logger.warn(f"{item.title} 未找到集媒体项：{episode.get('Id')}")
                    continue
                if self.__need_scrape(episodeinfo):
                    # 豆瓣演员表只在确实有集需要处理时才查询
                    if season_actors is None:
                        season_actors = self._resolver.douban_actors(mediainfo, season_item.get("IndexNumber"))
                    self.update_peoples(server, server_type, episode.get("Id"), episodeinfo, season_actors)
                    logger.info(f"集 {episodeinfo.get('Id')} 的人物信息更新完成")
                else:
                    logger.info(f"集 {episodeinfo.get('Id')} 的人物信息已是中文，无需更新")

    def update_peoples(self, server: str, server_type: str, itemid: str,
                       iteminfo: dict, douban_actors: List[dict]) -> None:
        """
        更新媒体项中的人物列表。

        "People": [{"Name": "丹尼尔·克雷格", "Id": "33625", "Role": "James Bond", "Type": "Actor"}]
        """
        peoples = []
        for people in iteminfo.get("People", []) or []:
            if self._stop_event.is_set():
                logger.info("演职人员刮削服务停止")
                return
            if not people.get("Name"):
                continue
            if StringUtils.is_chinese(people.get("Name")) and StringUtils.is_chinese(people.get("Role")):
                peoples.append(people)
                continue
            info = self.update_people(server, server_type, people, douban_actors)
            if info:
                peoples.append(info)
            elif not self._remove_nozh:
                peoples.append(people)
        if peoples or self._remove_nozh:
            iteminfo["People"] = peoples
            self.set_iteminfo(server, server_type, itemid, iteminfo)

    def update_people(self, server: str, server_type: str, people: dict,
                      douban_actors: Optional[List[dict]] = None) -> Optional[dict]:
        """
        更新人物详情和头像，返回替换后的媒体项人物，未获取到中文信息时返回 None。
        """
        ret_people = copy.deepcopy(people)
        try:
            personinfo = self.get_iteminfo(server, server_type, people.get("Id"))
            if not personinfo:
                logger.debug(f"未找到人物 {people.get('Name')} 的信息")
                return None
            provider_ids = {str(k).lower(): v for k, v in (personinfo.get("ProviderIds") or {}).items()}
            ref = PersonRef(
                name=people.get("Name"),
                role=people.get("Role"),
                tmdb_id=provider_ids.get("tmdb"),
                imdb_id=provider_ids.get("imdb"),
            )
            # 优先用 TMDB/IMDB ID 去重，避免同一演员在多集重复刮削
            person_key = f"{server}:{ref.tmdb_id or ref.imdb_id or people.get('Id') or ref.name}"
            info = self._resolver.resolve(ref, douban_actors)

            if info.image_url:
                # 图片下载和上传成本最高，同一轮同一人物只执行一次
                if person_key not in self._processed_images:
                    self._processed_images.add(person_key)
                    self.set_item_image(server, server_type, people.get("Id"), info.image_url)
                else:
                    logger.debug(f"人物 {ref.name} 的图片本轮已更新，跳过")

            if not info.found:
                logger.debug(f"人物 {ref.name} 未找到中文数据")
                return None
            locked_fields = personinfo.setdefault("LockedFields", [])
            if info.name:
                personinfo["Name"] = info.name
                ret_people["Name"] = info.name
                if "Name" not in locked_fields:
                    locked_fields.append("Name")
            if info.overview:
                personinfo["Overview"] = info.overview
                if "Overview" not in locked_fields:
                    locked_fields.append("Overview")
            if info.role:
                ret_people["Role"] = info.role
            # 人物详情是全局对象，同一轮已写入后无需按每集重复写
            if person_key in self._processed_infos:
                logger.debug(f"人物 {ref.name} 的信息本轮已更新，跳过")
                return ret_people
            if self.set_iteminfo(server, server_type, people.get("Id"), personinfo):
                self._processed_infos.add(person_key)
                return ret_people
        except Exception as err:
            logger.error(f"更新人物信息失败：{err}")
        return None

    def __need_scrape(self, iteminfo: dict) -> bool:
        """媒体项中是否有按刮削条件需要处理的人物。"""
        return any(need_scrape(p.get("Name"), p.get("Role"), self._person_type)
                   for p in iteminfo.get("People", []) or [])

    def __service(self, server: str) -> Optional[ServiceInfo]:
        """获取媒体服务器实例。"""
        service = self.services().get(server)
        if not service:
            logger.warn(f"未找到媒体服务器 {server} 的实例")
        return service

    def get_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        """获得媒体项详情。"""
        service = self.__service(server)
        if not service or not itemid:
            return {}
        if server_type == "emby":
            url = f"[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo&api_key=[APIKEY]"
        else:
            url = f"[HOST]Users/[USER]/Items/{itemid}?Fields=ChannelMappingInfo&api_key=[APIKEY]"
        try:
            res = service.instance.get_data(url=url)
            if res:
                return res.json() or {}
        except Exception as err:
            logger.error(f"获取{server}媒体项详情失败：{err}")
        return {}

    def get_items(self, server: str, server_type: str, parentid: str) -> dict:
        """获得媒体的所有子媒体项。"""
        service = self.__service(server)
        if not service or not parentid:
            return {}
        if server_type == "emby":
            url = f"[HOST]emby/Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]"
        else:
            url = f"[HOST]Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]"
        try:
            res = service.instance.get_data(url=url)
            if res:
                return res.json() or {}
        except Exception as err:
            logger.error(f"获取{server}媒体的所有子媒体项失败：{err}")
        return {}

    def set_iteminfo(self, server: str, server_type: str, itemid: str, iteminfo: dict) -> bool:
        """更新媒体项详情。"""
        service = self.__service(server)
        if not service:
            return False
        if server_type == "emby":
            url = f"[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json"
        else:
            url = f"[HOST]Items/{itemid}?api_key=[APIKEY]"
        try:
            res = service.instance.post_data(url=url, data=json.dumps(iteminfo),
                                             headers={"Content-Type": "application/json"})
            if res is not None and res.status_code in (200, 204):
                return True
            logger.error(f"更新{server}媒体项详情失败，错误码：{res.status_code if res is not None else '无响应'}")
        except Exception as err:
            logger.error(f"更新{server}媒体项详情失败：{err}")
        return False

    def set_item_image(self, server: str, server_type: str, itemid: str, imageurl: str) -> bool:
        """
        更新人物头像：由插件下载图片（走插件代理、重试和备用域名）后上传，
        Jellyfin 上传失败时再让服务器自行下载。
        """
        service = self.__service(server)
        if not service:
            return False
        logger.debug(f"更新人物 {itemid} 的图片：{imageurl}")
        with self._downloader.fetch(imageurl) as image:
            if image:
                prefix = "[HOST]emby/" if server_type == "emby" else "[HOST]"
                try:
                    res = service.instance.post_data(
                        url=f"{prefix}Items/{itemid}/Images/Primary?api_key=[APIKEY]",
                        data=base64.b64encode(image.read_bytes()).decode(),
                        headers={"Content-Type": image.content_type},
                    )
                    if res is not None and res.status_code in (200, 204):
                        return True
                    logger.error(f"上传{server}人物图片失败，错误码：{res.status_code if res is not None else '无响应'}")
                except Exception as err:
                    logger.error(f"上传{server}人物图片失败：{err}")
        if server_type != "jellyfin":
            return False
        try:
            res = service.instance.post_data(
                url=f"[HOST]Items/{itemid}/RemoteImages/Download?Type=Primary"
                    f"&ImageUrl={quote(imageurl, safe='')}&ProviderName=TheMovieDb&api_key=[APIKEY]"
            )
            if res is not None and res.status_code in (200, 204):
                return True
            logger.error(f"{server}远程下载人物图片失败，错误码：{res.status_code if res is not None else '无响应'}")
        except Exception as err:
            logger.error(f"{server}远程下载人物图片失败：{err}")
        return False
