"""Convert a CP1252 byte string containing HTML entities to Unicode."""

from html import unescape


def htmlentitydecode(string_cp1252: bytes) -> str:
    """
    decode the input-string, which is supposed to be CP1252
    and replace some special characters
    """
    try:
        ustr = unescape(string_cp1252.decode('cp1252'))
    except UnicodeDecodeError:
        ustr = string_cp1252
    return ustr
