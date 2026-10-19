"""
NFO 演职人员刮削：把主程序生成的 NFO 中的演员改为中文名称和饰演角色，并把头像保存到本地 .actors 目录。

Kodi 直接读取 .actors 目录；Jellyfin/Emby 不读取该目录，只认 <actor><thumb>，
因此本地模式下 <thumb> 写入头像文件在媒体服务器中看到的路径（可配置路径映射）。
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from xml.dom import minidom

from app import schemas
from app.schemas import MediaInfo
from app.schemas.types import ScrapingTarget
from app.sdk.logging import logger

from .network import ImageDownloader
from .person import PERSON_TYPE_ALL, PersonRef, PersonResolver, need_scrape

# 演员头像保存方式
ACTOR_THUMB_LOCAL = "local"
ACTOR_THUMB_URL = "url"

# Kodi 约定的演员头像目录
ACTORS_DIR = ".actors"

_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".webp")


def parse_path_mapping(value: Optional[str]) -> List[Tuple[str, str]]:
    """
    解析每行一个的路径映射：MoviePilot路径:媒体服务器路径，兼容 Windows 盘符。

    :param value: 表单输入
    :return: (MoviePilot路径, 媒体服务器路径) 列表，较长的源路径优先匹配
    """
    mappings = []
    for line in (value or "").split("\n"):
        line = line.strip()
        if not line:
            continue
        matched = re.match(r"^((?:[A-Za-z]:)?[^:]*):((?:[A-Za-z]:)?.*)$", line)
        if not matched or not matched.group(1).strip() or not matched.group(2).strip():
            logger.warn(f"路径映射格式错误，应为 MoviePilot路径:媒体服务器路径：{line}")
            continue
        mappings.append((matched.group(1).strip().rstrip("/\\"), matched.group(2).strip().rstrip("/\\")))
    return sorted(mappings, key=lambda m: len(m[0]), reverse=True)


def map_path(path: Union[str, Path], mappings: List[Tuple[str, str]]) -> str:
    """把 MoviePilot 中的文件路径转换为媒体服务器中看到的路径，未匹配映射时原样返回。"""
    path = str(path).replace("\\", "/")
    for source, target in mappings:
        source = source.replace("\\", "/")
        if path == source or path.startswith(f"{source}/"):
            mapped = target + path[len(source):]
            # 目标是 Windows 路径时使用反斜杠
            return mapped.replace("/", "\\") if re.match(r"^[A-Za-z]:", target) else mapped.replace("\\", "/")
    return path


def actor_filename(name: str, url: str) -> str:
    """按 Kodi 约定生成头像文件名：空格替换为下划线，去掉文件名非法字符。"""
    stem = re.sub(r'[\\/:*?"<>|]', "", name).strip().replace(" ", "_") or "unknown"
    suffix = Path(url.split("?")[0]).suffix.lower()
    return stem + (suffix if suffix in _IMAGE_SUFFIXES else ".jpg")


class NfoActorWriter:
    """
    处理一份 NFO 中的 <actor> 节点：中文化名称和角色、保存头像并改写 <thumb>。

    一轮刮削任务创建一个实例，人物信息和已保存的头像在本轮内复用，
    同一个人在电影、剧集和各集 NFO 中的名称保持一致。
    """

    def __init__(self, storagechain, downloader: ImageDownloader, resolver: PersonResolver,
                 localize: bool = True, thumb_mode: str = ACTOR_THUMB_LOCAL,
                 path_mappings: Optional[List[Tuple[str, str]]] = None, overwrite_images: bool = False):
        """
        :param storagechain: 存储处理链，用于保存头像
        :param downloader: 图片下载器
        :param resolver: 演职人员中文信息解析器
        :param localize: 是否把演员名称和角色改为中文
        :param thumb_mode: local 下载头像到 .actors 目录，url 保留远程地址
        :param path_mappings: 写入 <thumb> 时的路径映射
        :param overwrite_images: 是否覆盖已存在的头像
        """
        self._storagechain = storagechain
        self._downloader = downloader
        self._resolver = resolver
        self._localize = localize
        self._thumb_mode = thumb_mode
        self._path_mappings = path_mappings or []
        self._overwrite_images = overwrite_images
        # 本轮已处理的头像：本地路径 -> 是否可用
        self._saved_images: Dict[str, bool] = {}

    def process(self, content: Union[str, bytes], actors_root: schemas.FileItem, mediainfo: Optional[MediaInfo],
                item_type: ScrapingTarget, season: Optional[int] = None) -> Union[str, bytes]:
        """
        处理 NFO 内容中的演员，解析失败或没有演员时原样返回。

        :param content: 主程序生成的 NFO 内容
        :param actors_root: .actors 所在目录：电影目录或剧集根目录
        :param mediainfo: 媒体信息，用于查询豆瓣演员表
        :param item_type: 刮削目标类型
        :param season: 季号，单集 NFO 按季查询豆瓣演员表
        :return: 处理后的 NFO 内容，类型与输入一致
        """
        if not content or item_type not in (ScrapingTarget.MOVIE, ScrapingTarget.TV, ScrapingTarget.EPISODE):
            return content
        try:
            doc = minidom.parseString(content.encode("utf-8") if isinstance(content, str) else content)
        except Exception as err:
            logger.warn(f"NFO 解析失败，跳过演职人员刮削：{err}")
            return content
        actors = [node for node in doc.documentElement.childNodes
                  if node.nodeType == node.ELEMENT_NODE and node.tagName == "actor"]
        if not actors:
            return content

        douban_actors = None
        changed = False
        for actor in actors:
            name = self.__text(actor, "name")
            if not name:
                continue
            role = self.__text(actor, "role")
            thumb = self.__text(actor, "thumb")
            image_url = thumb
            if self._localize and need_scrape(name, role, PERSON_TYPE_ALL):
                if douban_actors is None:
                    douban_actors = self._resolver.douban_actors(
                        mediainfo, season if item_type == ScrapingTarget.EPISODE else None
                    ) if mediainfo else []
                info = self._resolver.resolve(
                    PersonRef(name=name, role=role, tmdb_id=self.__text(actor, "tmdbid")), douban_actors
                )
                if info.name and info.name != name:
                    logger.info(f"NFO 演员 {name} 更新为 {info.name}")
                    self.__set_text(doc, actor, "name", info.name)
                    name = info.name
                    changed = True
                if info.role and info.role != role:
                    self.__set_text(doc, actor, "role", info.role)
                    changed = True
                image_url = image_url or info.image_url
            if not image_url:
                continue
            if self._thumb_mode == ACTOR_THUMB_LOCAL:
                image_path = self.__save_image(actors_root, name, image_url)
                if image_path:
                    image_url = map_path(image_path, self._path_mappings)
            if image_url != thumb:
                self.__set_text(doc, actor, "thumb", image_url)
                changed = True

        if not changed:
            return content
        self.__strip_whitespace(doc)
        result = doc.toprettyxml(indent="  ", encoding="utf-8")
        return result.decode("utf-8") if isinstance(content, str) else result

    def __save_image(self, actors_root: schemas.FileItem, name: str, url: str) -> Optional[Path]:
        """
        下载头像到 .actors 目录，已存在时直接复用。

        :return: 头像文件路径，保存失败时为 None
        """
        image_path = Path(actors_root.path) / ACTORS_DIR / actor_filename(name, url)
        key = f"{actors_root.storage}:{image_path.as_posix()}"
        if key in self._saved_images:
            return image_path if self._saved_images[key] else None
        if not self._overwrite_images and self._storagechain.get_file_item(storage=actors_root.storage,
                                                                           path=image_path):
            self._saved_images[key] = True
            return image_path
        saved = False
        with self._downloader.fetch(url, suffix=image_path.suffix) as image:
            if image:
                folder = self._storagechain.get_folder(storage=actors_root.storage, path=image_path.parent)
                if folder and self._storagechain.upload_file(fileitem=folder, path=image.path,
                                                             new_name=image_path.name):
                    logger.info(f"已保存演员头像：{image_path}")
                    saved = True
                else:
                    logger.warn(f"演员头像保存失败：{image_path}")
        self._saved_images[key] = saved
        return image_path if saved else None

    @staticmethod
    def __text(node: minidom.Element, tag: str) -> Optional[str]:
        """读取直属子节点的文本。"""
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE and child.tagName == tag:
                text = "".join(c.data for c in child.childNodes
                               if c.nodeType in (c.TEXT_NODE, c.CDATA_SECTION_NODE)).strip()
                return text or None
        return None

    @staticmethod
    def __set_text(doc: minidom.Document, node: minidom.Element, tag: str, value: str) -> None:
        """设置直属子节点文本，不存在时创建；新建的 role 紧跟在 name 之后。"""
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE and child.tagName == tag:
                while child.firstChild:
                    child.removeChild(child.firstChild)
                child.appendChild(doc.createTextNode(value))
                return
        element = doc.createElement(tag)
        element.appendChild(doc.createTextNode(value))
        anchor = None
        if tag == "role":
            names = [c for c in node.childNodes if c.nodeType == c.ELEMENT_NODE and c.tagName == "name"]
            anchor = names[0].nextSibling if names else None
        node.insertBefore(element, anchor)

    @classmethod
    def __strip_whitespace(cls, node: minidom.Node) -> None:
        """去掉排版产生的空白文本节点，避免重新格式化时出现多余空行。"""
        for child in list(node.childNodes):
            if child.nodeType == child.TEXT_NODE and not child.data.strip():
                node.removeChild(child)
            elif child.hasChildNodes():
                cls.__strip_whitespace(child)
