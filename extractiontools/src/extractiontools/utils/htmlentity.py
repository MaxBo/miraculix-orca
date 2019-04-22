"""convert bytestring with html to unicode"""

from html.entities import entitydefs, codepoint2name, name2codepoint

name2codepoint['apos'] = 39
entitydefs['apos'] = '\x27'
codepoint2name[39] = 'apos'


def htmlentitydecode(string_cp1252: bytes) -> str:
    """
    decode the input-string, which is supposed to be CP1252
    and replace some special characters
    """
    try:
        ustr = string_cp1252.decode('cp1252')
        for key, value in entitydefs.items():
            if value.startswith('&'):
                ustr = ustr.replace(value, chr(name2codepoint[key]))
        for key in codepoint2name:
            ustr = ustr.replace(f'&#{key};', chr(key))
    except UnicodeDecodeError:
        ustr = string_cp1252
    return ustr
