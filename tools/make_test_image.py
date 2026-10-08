"""
Generate colourful test images (so blur / black & white are easy to see).

    docker compose run --rm -v "$PWD/samples:/app/samples" master python -m tools.make_test_image
"""
import cv2
import numpy as np

SIZES = {"small_1024": (1024, 1024), "medium_2048": (2048, 2048), "large_4096x3072": (4096, 3072)}

for name, (w, h) in SIZES.items():
    x = np.linspace(0, 255, w, dtype=np.uint8)
    y = np.linspace(0, 255, h, dtype=np.uint8)
    img = np.dstack([np.tile(x, (h, 1)), np.tile(y[:, None], (1, w)),
                     np.full((h, w), 160, np.uint8)]).copy()           # colour gradient
    for i in range(0, w, 128):                                          # sharp grid lines
        cv2.line(img, (i, 0), (i, h), (255, 255, 255), 2)
    for j in range(0, h, 128):
        cv2.line(img, (0, j), (w, j), (255, 255, 255), 2)
    cv2.circle(img, (w // 2, h // 2), min(w, h) // 4, (0, 0, 255), -1)
    cv2.putText(img, name, (60, 160), cv2.FONT_HERSHEY_SIMPLEX, 5, (20, 20, 20), 12)
    cv2.imwrite(f"samples/{name}.jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 92])
    print("wrote", f"samples/{name}.jpg")
