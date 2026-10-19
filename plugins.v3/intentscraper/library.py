"""
媒体库文件刮削：在主程序刮削链上排除 Jellyfin 预览缓存，并替换为加固的图片下载。
"""

from pathlib import Path

from app import schemas
from app.chain.scraping import ScrapingChain
from app.sdk.logging import logger

from .network import ImageDownloader


def is_trickplay_path(path: Path) -> bool:
    """按目录路径分量排除 Jellyfin 预览缓存及其子目录，保留同名媒体文件。"""
    return any(parent.name.lower().endswith(".trickplay") for parent in path.parents) or (
        path.name.lower().endswith(".trickplay") and path.is_dir()
    )


class LibraryScrapingChain(ScrapingChain):
    """
    为媒体库刮削的递归调用保留 Jellyfin 预览目录排除规则，并替换图片下载逻辑。
    """

    # 由插件 init_plugin 写入，与演职人员刮削共用同一个下载器
    downloader: ImageDownloader = ImageDownloader()

    def scrape_metadata(self, fileitem, *args, **kwargs):
        """在每次递归写入前检查目录，防止带季号的缓存被当作剧集目录。"""
        if fileitem and is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)

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
