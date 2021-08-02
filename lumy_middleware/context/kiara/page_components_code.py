import os
from hashlib import blake2b
from typing import List
from urllib.parse import urlparse
from urllib.request import urlopen

from lumy_middleware.types.generated import Code, LumyWorkflow


def content_hash(content: str) -> str:
    h = blake2b(digest_size=10)
    h.update(str.encode(content))
    return h.hexdigest()


def load_from_file(filepath: str) -> str:
    path = os.path.abspath(filepath)
    with open(path, 'r') as f:
        return f.read()


def load_from_network(url: str) -> str:
    return urlopen(url).read()


def load_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'file':
        return load_from_file(url.replace('file://', ''))
    return load_from_network(url)


def get_specific_components_code(workflow: LumyWorkflow) -> List[Code]:
    pages = workflow.ui.pages or []
    urls: List[str] = [
        p.component.url
        for p in pages
        if p.component.url is not None
    ]
    contents = [
        load_url(url)
        for url in urls
    ]
    return [
        Code(content=c, id=content_hash(c))
        for c in contents
    ]
