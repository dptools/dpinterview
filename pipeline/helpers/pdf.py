"""
Helper functions for drawing on PDF canvases
"""

from pathlib import Path
from typing import List, Optional, Tuple

from reportlab.graphics import renderPDF
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from svglib.svglib import svg2rlg

# params needed to scale reportlab coordinates to match Illustrator coordinates
iw = 819
ih = 1056
w, h = letter  # report pdf is standard letter dimensions
cw = w / iw
ch = h / ih


def draw_svg(
    canvas: canvas.Canvas,
    svg_path: Path,
    x: float,
    y: float,
    width: float,
    height: float,
):
    """
    Draw an SVG image onto a PDF canvas.

    Args:
        canvas (canvas.Canvas): The PDF canvas to draw on.
        svg_path (Path): The path to the SVG image file.
        x (float): The x-coordinate of the top-left corner of the image.
        y (float): The y-coordinate of the top-left corner of the image.
        width (float): The width of the image.
        height (float): The height of the image.
    """
    drawing = svg2rlg(svg_path)

    if drawing is None:
        return

    xL, yL, xH, yH = drawing.getBounds()  # type: ignore

    drawing.renderScale = cw * width / (xH - xL)
    drawing.renderScale = ch * height / (yH - yL)

    renderPDF.draw(drawing, canvas, x * cw, h - y * ch)


def draw_image(
    canvas: canvas.Canvas,
    image_path: Path,
    x: float,
    y: float,
    width: float,
    height: float,
):
    """
    Draw an image on the canvas.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        image_path (Path): The path to the image file.
        x (float): The x-coordinate of the lower-left corner of the image.
        y (float): The y-coordinate of the lower-left corner of the image.
        width (float): The width of the image.
        height (float): The height of the image.
    """
    canvas.drawImage(image_path, x * cw, h - y * ch, width * cw, height * ch)


def draw_text(
    canvas: canvas.Canvas,
    text: str,
    x: Optional[float],
    y: float,
    size: float,
    font: str,
    color: Tuple[float, float, float, float] = (0, 0, 0, 1),
    preprocess: bool = True,
    center: bool = False,
):
    """
    Draw text on a PDF canvas.

    Args:
        canvas (canvas.Canvas): The PDF canvas to draw on.
        text (str): The text to draw.
        x (Optional[float]): The x-coordinate of the text.
            If `center` is True, this argument is ignored.
        y (float): The y-coordinate of the text.
        size (float): The font size.
        font (str): The font to use.
        color (Tuple[float, float, float, float], optional):
            The color of the text. Defaults to (0, 0, 0, 1).
        preprocess (bool, optional): Whether to preprocess the coordinates. Defaults to True.
        center (bool, optional): Whether to center the text horizontally. Defaults to False.
    """
    canvas.setFillColorRGB(*color)

    if center:
        width = stringWidth(text, font, size)
        x = (w - width) / 2.0

    if preprocess:
        pdf_text_object = canvas.beginText(x * cw, h - y * ch)  # type: ignore
    else:
        pdf_text_object = canvas.beginText(x, y)  # type: ignore
    pdf_text_object.setFont(font, size)
    pdf_text_object.textOut(text)

    canvas.drawText(pdf_text_object)

    canvas.setFillColorRGB(0, 0, 0, 1)


def draw_text_vertical_centered(
    canvas: canvas.Canvas,
    text: str,
    y1: float,
    y2: float,
    x: float,
    size: float,
    font: str,
    angle: float = 90,
):
    """
    Draw text vertically centered on a canvas.

    Draws text centered between the given y-coordinates.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        text (str): The text to draw.
        y1 (float): The starting y-coordinate of the text box.
        y2 (float): The ending y-coordinate of the text box.
        x (float): The x-coordinate of the text box.
        size (float): The font size.
        font (str): The font to use.
        angle (float, optional): The angle to rotate the text box.
            Only tested for 90 degrees. Defaults to 90.
    """
    canvas.setFillColorRGB(0, 0, 0, 1)

    text_width = stringWidth(text, font, size)

    match angle:
        case 90:
            _x = y2 - (y2 - y1) / 2.0 + text_width / float(2 * ch)

            _x = h - _x * ch
            _y = -x * cw

        case _:
            raise NotImplementedError("Only 90 degrees is supported.")

    canvas.rotate(angle)
    draw_text(canvas, text, _x, _y, size, font, preprocess=False)
    canvas.rotate(-angle)


def draw_text_vertical_centered_270(
    canvas: canvas.Canvas,
    text: str,
    y: float,
    x: float,
    size: float,
    font: str,
    angle: float = 270,
):
    """
    Draw text vertically centered at a given position and angle on a canvas.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        text (str): The text to draw.
        y (float): The y-coordinate of the center of the text.
        x (float): The x-coordinate of the center of the text.
        size (float): The font size to use.
        font (str): The name of the font to use.
        angle (float, optional): The angle to rotate the text by, in degrees.
            Only tested for 270 degrees.Defaults to 270.
    """
    canvas.setFillColorRGB(0, 0, 0, 1)

    text_width = stringWidth(text, font, size)

    match angle:
        case 270:
            _x = y - text_width / float(2 * ch)

            _x = _x * ch - h
            _y = x * cw

        case _:
            raise NotImplementedError("Only 270 degrees is supported.")

    canvas.rotate(angle)
    draw_text(canvas, text, _x, _y, size, font, preprocess=False)
    canvas.rotate(-angle)


def draw_colored_rect(
    canvas: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    height: float,
    color: Tuple[float, float, float, float],
    fill: bool = True,
    stroke: bool = False,
    line_width: Optional[float] = None,
):
    """
    Draw a colored rectangle on a ReportLab canvas.

    Args:
        canvas (reportlab.pdfgen.canvas.Canvas): The canvas to draw on.
        x (float): The x-coordinate of the lower-left corner of the rectangle.
        y (float): The y-coordinate of the lower-left corner of the rectangle.
        width (float): The width of the rectangle.
        height (float): The height of the rectangle.
        color (Tuple[float, float, float, float]): The color of the rectangle in RGBA format.
        fill (bool, optional): Whether to fill the rectangle with the given color.
            Defaults to True.
        stroke (bool, optional): Whether to draw the outline of the rectangle.
            Defaults to False.
        line_width (Optional[float], optional): The width of the rectangle's outline.
            Defaults to None / 1.
    """
    if line_width:
        canvas.setLineWidth(line_width)

    canvas.setFillColorRGB(*(tuple(color)))
    canvas.rect(x * cw, h - y * ch, width * cw, height * ch, fill=fill, stroke=stroke)
    canvas.setFillColorRGB(0, 0, 0, 1)


def compute_x_right_align(text: str, font: str, size: float, x: float) -> float:
    """
    Computes the x-coordinate for right-aligning text in a PDF document.

    Args:
        text (str): The text to be right-aligned.
        font (str): The font to be used for the text.
        size (float): The font size to be used for the text.
        x (float): The initial x-coordinate for the text.

    Returns:
        float: The x-coordinate for right-aligning the text.
    """
    text_width = stringWidth(text, font, size)
    return x - text_width / float(cw)


def compute_x_right_align_list(
    text_list: List[str], font: str, size: float, x: float
) -> List[float]:
    """
    Computes the x-coordinate for right-aligned text for each string in the given list.

    Args:
        text_list (List[str]): List of strings to compute the x-coordinate for.
        font (str): Font to use for the text.
        size (float): Font size to use for the text.
        x (float): Starting x-coordinate for the text.

    Returns:
        List[float]: List of x-coordinates for right-aligned text for each string in the given list.
    """
    return [compute_x_right_align(text, font, size, x) for text in text_list]


def draw_line(
    canvas: canvas.Canvas, x1: float, y1: float, x2: float, y2: float, line_width: float
):
    """
    Draw a line on the given canvas object from (x1, y1) to (x2, y2) with the given line width.

    Args:
        canvas (canvas.Canvas): The canvas object to draw the line on.
        x1 (float): The x-coordinate of the starting point of the line.
        y1 (float): The y-coordinate of the starting point of the line.
        x2 (float): The x-coordinate of the ending point of the line.
        y2 (float): The y-coordinate of the ending point of the line.
        line_width (float): The width of the line to be drawn.
    """
    canvas.setLineWidth(line_width)
    canvas.line(x1 * cw, h - y1 * ch, x2 * cw, h - y2 * ch)
