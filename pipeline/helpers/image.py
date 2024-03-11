"""
Provides helper functions for image processing.
"""

from pathlib import Path
from typing import List, Tuple
import math

import cv2


def check_if_image_has_black_bars(
    image_file: Path, bars_height: float = 0.2, threshold: float = 0.8
) -> bool:
    """
    Checks if an image has black bars.

    Checks if top and bottom {threshold} of the image has black pixels, if majority of the
    pixels are black, then the image has black bars.

    Args:
        image_file (Path): Path to image file
        bars_height (float, optional): Height of the top and bottom bars. Defaults to 0.2.
            - 0.2 means 20% of the image height
        threshold (float, optional): Threshold to determine if the image has black bars.
            Defaults to 0.8.
            - If the ratio of black pixels to total pixels is greater than the threshold,
            then the image has black bars.

    Returns:
        bool: True if image has black bars, False otherwise
    """
    image = cv2.imread(str(image_file))
    height, width, _ = image.shape  # height, width, channels

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Get top and bottom bars
    bar_height = int(height * bars_height)
    top_pixels = gray[0:bar_height, 0:width]
    bottom_pixels = gray[height - bar_height: height, 0:width]

    pixels_count = width * bar_height

    top_black_pixels = pixels_count - cv2.countNonZero(top_pixels)
    bottom_black_pixels = pixels_count - cv2.countNonZero(bottom_pixels)

    total_black_pixels = top_black_pixels + bottom_black_pixels
    total_pixels = pixels_count * 2

    if total_black_pixels / total_pixels > threshold:
        return True
    else:
        return False


def get_black_bars_height(image_file: Path) -> float:
    """
    Gets the height of the black bars in the image.

    Args:
        image_file (Path): Path to image file
    """
    image = cv2.imread(str(image_file))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # apply threshold to invert the image
    _, thresh = cv2.threshold(gray, 3, 255, cv2.THRESH_BINARY_INV)

    # find contours of the white regions
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    heights_y: List[Tuple[int, int]] = []
    y_heights: List[Tuple[int, int]] = []

    # loop over the contours
    for c in contours:
        # get the bounding rectangle of the contour
        _, y, _, h = cv2.boundingRect(c)  # x, y, w, h
        if h < 100:
            continue
        heights_y.append((h, y))
        y_heights.append((y, h))

    heights_y.sort(key=lambda x: x[0], reverse=False)
    y_heights.sort(key=lambda x: x[0], reverse=False)

    if y_heights[0][0] == 0:
        return y_heights[0][1]
    else:
        # average heights or 2 highest bars
        if len(heights_y) > 1:
            height = math.ceil((heights_y[0][0] + heights_y[1][0]) / 2)
        else:
            height = heights_y[0][0]
        return height


def get_frame_by_number(
    video_path: Path,
    frame_number: int,
    dest_image: Path,
):
    """
    Gets a frame from a video by frame number.

    Args:
        video_path (Path): The path to the video.
        frame_number (int): The frame number to get.
        dest_image (Path): The path to save the frame.
    """

    # Reference:
    # https://stackoverflow.com/questions/33650974/opencv-python-read-specific-frame-using-videocapture

    # Read the video
    cap = cv2.VideoCapture(str(video_path))

    # Set the frame number
    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number - 1)

    # Read the frame
    (
        _,
        frame,
    ) = (
        cap.read()
    )  # ret, frame: ret is a boolean indicating if the frame was read successfully

    # Save the frame
    cv2.imwrite(str(dest_image), frame)


def draw_bars_over_image(
    source_image: Path,
    dest_image: Path,
    start_h: float = 0.1,
    end_h: float = 0.4,
    bar_color: Tuple[int, int, int] = (0, 0, 0),
):
    """
    Anonymizes an image by setting the eyes portion to black.

    Args:
        source_image (Path): The path to the source image.
        dest_image (Path): The path to save the anonymized image.
        start_h (float, optional): The starting height ratio of the image to be set to black.
            Defaults to 0.1.
        end_h (float, optional): The ending height ratio of the image to be set to black.
            Defaults to 0.4.
        bar_color (Tuple[int, int, int], optional): The color to set the bars to.
            Defaults to (0, 0, 0), which is black.
    """
    # Read the image
    img = cv2.imread(str(source_image))

    # Get the image dimensions (OpenCV stores image data as NumPy ndarray)
    height, _, _ = img.shape  # height, width, channels

    # Set all pixels between start_row and end_row to black
    start_row = int(height * start_h)
    end_row = int(height * end_h)
    img[start_row:end_row, :] = bar_color

    # Save the cropped image
    cv2.imwrite(str(dest_image), img)


def blur_image(source_image: Path, dest_image: Path, blur_kernel_size: int = 15):
    """
    Blurs an image.

    Args:
        source_image (Path): The path to the source image.
        dest_image (Path): The path to save the blurred image.
        blur_kernel_size (int, optional): The size of the blur kernel. Defaults to 15.
    """
    # Read the image
    img = cv2.imread(source_image)

    # Blur the image
    img = cv2.blur(img, (blur_kernel_size, blur_kernel_size))

    # Save the blurred image
    cv2.imwrite(dest_image, img)


def pad_image(
    source_image: Path,
    dest_image: Path,
    padding: int,
    padding_color: Tuple[int, int, int] = (0, 0, 0),
):
    """
    Pads an image.

    Args:
        source_image (Path): The path to the source image.
        dest_image (Path): The path to save the padded image.
        padding (int, optional): The padding size. Defaults to 50.
        padding_color (Tuple[int, int, int], optional): The color to use for padding.
            Defaults to (0, 0, 0), which is black.
    """
    # Read the image
    img = cv2.imread(str(source_image))

    # Get the image dimensions (OpenCV stores image data as NumPy ndarray)
    # height, width, _ = img.shape  # height, width, channels

    # Create a new image with the padding
    new_img = cv2.copyMakeBorder(
        img,
        padding,
        padding,
        padding,
        padding,
        cv2.BORDER_CONSTANT,
        value=padding_color,
    )

    # Save the padded image
    cv2.imwrite(str(dest_image), new_img)
