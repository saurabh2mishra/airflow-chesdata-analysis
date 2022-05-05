import os, ssl
import urllib.request as req
from pathlib import Path

from conf import config


def _set_default_https_context():
    if not os.environ.get("PYTHONHTTPSVERIFY", "") and getattr(
        ssl, "_create_unverified_context", None
    ):
        ssl._create_default_https_context = ssl._create_unverified_context


def download_files(**kwargs):
    """
    Accept uri, file_name and download files in ./data location.
    :param String uri: uri
    :param String file_name: name of file.
    :return: None
    """
    _set_default_https_context()
    file_name = kwargs["file_name"]
    uri = kwargs["uri"]
    file_path = f"{config.parent_dir}/{file_name}"
    path = Path(file_path)
    if not path.is_file():
        opener = req.build_opener()
        opener.addheaders = [("User-agent", "Mozilla/5.0")]
        req.install_opener(opener)
        req.urlretrieve(uri, file_path)
    else:
        print(f"{file_name} already exits at folder {config.parent_dir}")
