import string



from langdetect import detect
import re


def isenglish(string):
    lang = detect(string)
    if (lang == 'en'):
        return True
    else:
        return False


def filterstring (string):
    string_striped = re.sub(r'!+(?=.*\!)','',string)
    return string_striped


def toUTF8(string):
    utf_string = string.decode('utf-8', 'ignore')
    return utf_string

def FindMentions(text):
    usernames_found = re.findall(r'\b@\w+', text)
    return usernames_found


def clearUsername(username):
    screenname = username.replace(username[:1], '')
    return screenname

def remove_emoji(string):
    emoji_pattern = re.compile("[\P{L}&&\P{D}&&\P{Z}&&\P{M}]", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)