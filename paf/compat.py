import sys

if sys.version_info.major == 2:
    fdopen_binary_mode = ""
    str_type = unicode
else:
    fdopen_binary_mode = "b"
    str_type = str
