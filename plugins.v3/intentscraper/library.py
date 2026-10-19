"""
媒体库文件刮削：在主程序刮削链上排除 Jellyfin 预览缓存，使用插件自己的刮削项，
替换为加固的图片下载，并在保存 NFO 前处理其中的演职人员。
"""

import threading
from pathlib import Path
from typing import Iterable, Optional, Union

from app import schemas
from app.chain.scraping import ScrapingChain, ScrapingConfig
from app.schemas.types import ScrapingPolicy, ScrapingTarget
from app.sdk.logging import logger
from app.sdk.media import MetaInfo

from .network import ImageDownloader
from .nfo import NfoActorWriter

# 可刮削的元数据：NFO、海报、背景图、Logo、光盘图、横幅图、缩略图、透明艺术图、横版缩略图
SCRAPE_ITEMS = ("nfo", "poster", "backdrop", "logo", "disc", "banner", "thumb", "clearart", "landscape")


def build_policies(items: Iterable[str], nfo_overwrite: bool = False) -> ScrapingConfig:
    """
    按插件选择的刮削内容生成电影、电视剧、季和集的刮削策略，不再依赖系统的刮削开关。

    :param items: 需要刮削的元数据种类，未选择的种类跳过
    :param nfo_overwrite: NFO 是否始终覆盖，图片仍只补缺
    """
    items = set(items)
    config = {}
    for key in ScrapingConfig.get_default_config():
        target, metadata = key.split("_", 1)
        if target == "music":
            continue
        if metadata not in items:
            config[key] = ScrapingPolicy.SKIP
        elif metadata == "nfo" and nfo_overwrite:
            config[key] = ScrapingPolicy.OVERWRITE
        else:
            config[key] = ScrapingPolicy.MISSINGONLY
    return ScrapingConfig(config)


def is_trickplay_path(path: Path) -> bool:
    """按目录路径分量排除 Jellyfin 预览缓存及其子目录，保留同名媒体文件。"""
    return any(parent.name.lower().endswith(".trickplay") for parent in path.parents) or (
        path.name.lower().endswith(".trickplay") and path.is_dir()
    )


class LibraryScrapingChain(ScrapingChain):
    """
    为媒体库刮削的递归调用保留 Jellyfin 预览目录排除规则，使用插件的刮削策略，
    替换图片下载逻辑，并在保存 NFO 前处理演职人员。
    """

    # 以下由插件写入：与演职人员刮削共用同一个下载器
    downloader: ImageDownloader = ImageDownloader()
    # 插件的刮削策略
    policies: ScrapingConfig = build_policies(SCRAPE_ITEMS)
    # 本轮 NFO 演职人员处理器，为 None 时 NFO 原样保存
    nfo_writer: Optional[NfoActorWriter] = None

    # 当前线程正在生成的 NFO：(媒体信息, 刮削目标, 季号)
    _nfo_context = threading.local()

    @property
    def scraping_policies(self) -> ScrapingConfig:
        """始终使用插件的刮削策略。"""
        return self.policies

    @scraping_policies.setter
    def scraping_policies(self, value: ScrapingConfig) -> None:
        """忽略主程序按系统刮削开关初始化和重载的策略。"""

    def scrape_metadata(self, fileitem, *args, **kwargs):
        """在每次递归写入前检查目录，防止带季号的缓存被当作剧集目录。"""
        if fileitem and is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)

    def _scrape_nfo_generic(self, *args, **kwargs) -> None:
        """记录当前 NFO 所属的媒体和层级，供保存时处理演职人员。"""
        self._nfo_context.target = (kwargs.get("mediainfo"), kwargs.get("item_type"), kwargs.get("season_number"))
        try:
            super()._scrape_nfo_generic(*args, **kwargs)
        finally:
            self._nfo_context.target = None

    def _save_file(self, fileitem: schemas.FileItem, path: Path, content: Union[bytes, str]) -> None:
        """保存 NFO 前把演员改为中文并保存头像，其余文件原样保存。"""
        target = getattr(self._nfo_context, "target", None)
        writer = self.nfo_writer
        if writer and target and fileitem and path and path.suffix.lower() == ".nfo":
            mediainfo, item_type, season = target
            try:
                content = writer.process(content, actors_root=self.__actors_root(fileitem, item_type),
                                         mediainfo=mediainfo, item_type=item_type, season=season)
            except Exception as err:
                logger.error(f"{path.name} 演职人员刮削失败，按原内容保存：{err}")
        super()._save_file(fileitem=fileitem, path=path, content=content)

    def __actors_root(self, fileitem: schemas.FileItem, item_type: ScrapingTarget) -> schemas.FileItem:
        """
        .actors 目录所在位置：电影 NFO 所在目录；剧集和单集统一放在剧集根目录。

        :param fileitem: NFO 所在目录
        :param item_type: 刮削目标类型
        """
        if item_type != ScrapingTarget.EPISODE:
            return fileitem
        name = Path(fileitem.path).name
        if name in self.runtime_config.season_zero_names or MetaInfo(name).begin_season is not None:
            return self.storagechain.get_parent_item(fileitem) or fileitem
        return fileitem

    def _download_and_save_image(self, fileitem: schemas.FileItem, path: Path, url: str) -> None:
        """
        通过插件下载器下载图片，再保存到媒体文件所在存储。

        :param fileitem: 关联的媒体文件项
        :param path: 图片文件路径
        :param url: 图片下载URL
        """
        if not fileitem or not url or not path:
            return
        with self.downloader.fetch(url, suffix=path.suffix) as image:
            if not image:
                return
            item = self.storagechain.upload_file(fileitem=fileitem, path=image.path, new_name=path.name)
            if item:
                logger.info(f"已保存图片：{item.path}")
            else:
                logger.warn(f"图片保存失败：{path}")
