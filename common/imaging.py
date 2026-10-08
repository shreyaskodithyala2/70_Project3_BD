"""
Pure image logic (OpenCV + NumPy). No Kafka or Redis in here.

Tiling with a HALO
──────────────────
The original project blurred every 512x512 tile on its own. A 51x51 blur at a
tile's edge has no neighbouring pixels to average, so visible seams appeared
on the tile grid in the final image.

Fix: send each tile with HALO (= 25 px = kernel radius) extra pixels borrowed
from its neighbours. The worker blurs the padded tile, then crops the halo off.
Every core pixel was blurred with its real neighbours, so the seams disappear.

    ┌──────────────────────┐   padded tile sent to the worker
    │   halo (25px)        │
    │   ┌──────────────┐   │   <- crop = (left, top, width, height) tells the
    │   │  core 512x512│   │      worker where the core sits inside the padding
    │   └──────────────┘   │
    └──────────────────────┘
"""
from dataclasses import dataclass

import cv2
import numpy as np

from common import config


@dataclass
class Tile:
    index: int        # position in row-major order: 0,1,2... left->right, top->bottom
    x: int            # top-left corner of the CORE in the full image
    y: int
    width: int        # core size (edge tiles can be smaller than 512)
    height: int
    crop: tuple       # (left, top, width, height) of the core inside the padded tile
    data: bytes       # JPEG bytes of the PADDED tile -> becomes the Kafka message value


def decode(data: bytes):
    """bytes (jpg/png/...) -> NumPy array of shape (H, W, 3), BGR order."""
    arr = np.frombuffer(data, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("not a valid image file")
    return img


def encode_jpeg(img, quality=config.JPEG_QUALITY) -> bytes:
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise ValueError("JPEG encoding failed")
    return buf.tobytes()


def make_preview(img) -> bytes:
    h, w = img.shape[:2]
    scale = config.PREVIEW_MAX_SIDE / max(h, w)
    if scale < 1:
        img = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    return encode_jpeg(img, quality=80)


def grid_size(width, height, tile=config.TILE_SIZE):
    cols = (width + tile - 1) // tile    # ceil division
    rows = (height + tile - 1) // tile
    return cols, rows


def split_into_tiles(img, tile=config.TILE_SIZE, halo=config.HALO):
    """Cut the image into a grid of TILE x TILE cores, each padded with a halo."""
    H, W = img.shape[:2]
    tiles = []
    index = 0
    for y in range(0, H, tile):
        for x in range(0, W, tile):
            core_w = min(tile, W - x)
            core_h = min(tile, H - y)
            # padded window, clamped to the image border
            x0, y0 = max(0, x - halo), max(0, y - halo)
            x1, y1 = min(W, x + core_w + halo), min(H, y + core_h + halo)
            padded = img[y0:y1, x0:x1]
            tiles.append(Tile(
                index=index, x=x, y=y, width=core_w, height=core_h,
                crop=(x - x0, y - y0, core_w, core_h),
                data=encode_jpeg(padded),
            ))
            index += 1
    return tiles


def apply_filter(img, name):
    if name == "blur":
        return cv2.GaussianBlur(img, (config.BLUR_KERNEL, config.BLUR_KERNEL), 0)
    if name == "blackwhite":
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)  # back to 3 channels so every tile has the same shape
    raise ValueError(f"unknown filter {name!r}")


def crop_core(img, crop):
    left, top, w, h = crop
    return img[top:top + h, left:left + w]


def assemble(width, height, tiles_by_index, tile=config.TILE_SIZE):
    """Paste processed tiles back onto a blank canvas. Each tile's position is
    derived from its index, exactly like split_into_tiles numbered them."""
    cols, _ = grid_size(width, height, tile)
    canvas = np.zeros((height, width, 3), dtype=np.uint8)
    for index, data in tiles_by_index.items():
        part = decode(data)
        row, col = divmod(index, cols)
        y, x = row * tile, col * tile
        h, w = part.shape[:2]
        canvas[y:y + h, x:x + w] = part
    return canvas
