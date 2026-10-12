"""
imaging.py - all image processing (OpenCV + NumPy). No Kafka or Redis here.

Used by the master (split / reassemble) and the workers (filter / crop).

Halo: each tile is sent with 25px borrowed from its neighbours, so a blur at the
tile's edge sees real pixels. The worker blurs the padded tile, then crops the
halo off. Without it, visible seams appear on the 512px grid.

    ┌──────────────────────┐   padded tile (sent to the worker)
    │   halo (25px)        │
    │   ┌──────────────┐   │   crop = (left, top, width, height) of the core
    │   │ core 512x512 │   │          inside the padded tile
    │   └──────────────┘   │
    └──────────────────────┘
"""
from dataclasses import dataclass

import cv2
import numpy as np

from common import config


@dataclass
class Tile:
    """One tile produced by split_into_tiles()."""
    index: int        # position, row by row: 0, 1, 2 ... (left->right, top->bottom)
    x: int            # top-left corner of the core in the full image
    y: int
    width: int        # core size (edge tiles can be smaller than 512)
    height: int
    crop: tuple       # (left, top, width, height) of the core inside the padded tile
    data: bytes       # JPEG bytes of the padded tile -> the Kafka message value


def decode(data: bytes):
    """Image file bytes -> pixel array.
    Input:  bytes of a jpg/png/... file
    Output: NumPy array of shape (height, width, 3), BGR colour order
    Raises ValueError if the bytes are not an image."""
    arr = np.frombuffer(data, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("not a valid image file")
    return img


def encode_jpeg(img, quality=config.JPEG_QUALITY) -> bytes:
    """Pixel array -> JPEG bytes (what we put in Kafka and Redis)."""
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise ValueError("JPEG encoding failed")
    return buf.tobytes()


def make_preview(img) -> bytes:
    """Small thumbnail (longest side 480px) as JPEG bytes, for the feed."""
    h, w = img.shape[:2]
    scale = config.PREVIEW_MAX_SIDE / max(h, w)
    if scale < 1:
        img = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    return encode_jpeg(img, quality=80)


def grid_size(width, height, tile=config.TILE_SIZE):
    """How many tile columns and rows an image needs (rounded up).
    Example: grid_size(2048, 2048) -> (4, 4);  grid_size(1100, 1024) -> (3, 2)"""
    cols = (width + tile - 1) // tile    # ceiling division
    rows = (height + tile - 1) // tile
    return cols, rows


def split_into_tiles(img, tile=config.TILE_SIZE, halo=config.HALO):
    """Cut an image into 512x512 tiles, each padded with a 25px halo.
    Input:  img - pixel array (H, W, 3)
    Output: list of Tile, numbered row by row
    Example: 2048x2048 image -> 16 tiles; tile 5 = row 1, col 1, core at (512, 512),
             sent as a 562x562 padded JPEG with crop (25, 25, 512, 512)."""
    H, W = img.shape[:2]
    tiles = []
    index = 0
    for y in range(0, H, tile):
        for x in range(0, W, tile):
            core_w = min(tile, W - x)
            core_h = min(tile, H - y)
            # padded window = core + halo, clamped at the image border
            x0, y0 = max(0, x - halo), max(0, y - halo)
            x1, y1 = min(W, x + core_w + halo), min(H, y + core_h + halo)
            padded = img[y0:y1, x0:x1]
            tiles.append(Tile(
                index=index, x=x, y=y, width=core_w, height=core_h,
                crop=(x - x0, y - y0, core_w, core_h),   # where the core sits inside the padding
                data=encode_jpeg(padded),
            ))
            index += 1
    return tiles


def apply_filter(img, name):
    """Apply a filter to a (padded) tile.
    Input:  img - pixel array, name - "blur" or "blackwhite"
    Output: filtered pixel array, same size and 3 channels"""
    if name == "blur":
        return cv2.GaussianBlur(img, (config.BLUR_KERNEL, config.BLUR_KERNEL), 0)
    if name == "blackwhite":
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)  # back to 3 channels so all tiles match
    raise ValueError(f"unknown filter {name!r}")


def crop_core(img, crop):
    """Cut the halo off a processed tile.
    Input:  padded tile + crop (left, top, width, height)
    Output: just the core. Example: 562x562 tile, crop (25,25,512,512) -> 512x512."""
    left, top, w, h = crop
    return img[top:top + h, left:left + w]


def assemble(width, height, tiles_by_index, tile=config.TILE_SIZE):
    """Paste processed tiles back into one image.
    Input:  final width/height, {tile index: JPEG bytes of the processed core}
    Output: full pixel array
    A tile's position comes from its index, the same numbering split_into_tiles used.
    Example: 4 columns, tile 5 -> row 1, col 1 -> pasted at (x=512, y=512)."""
    cols, _ = grid_size(width, height, tile)
    canvas = np.zeros((height, width, 3), dtype=np.uint8)
    for index, data in tiles_by_index.items():
        part = decode(data)
        row, col = divmod(index, cols)
        y, x = row * tile, col * tile
        h, w = part.shape[:2]
        canvas[y:y + h, x:x + w] = part
    return canvas
