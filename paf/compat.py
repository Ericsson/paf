import sys

if sys.version_info.major == 2:
    fdopen_binary_mode = ""
    str_type = unicode
    FileNotFoundError = IOError
    def bytes_to_hex(ary):
        return ":".join(["%02x" % ord(b) for b in ary])
else:
    fdopen_binary_mode = "b"
    str_type = str
    FileNotFoundError = FileNotFoundError
    def bytes_to_hex(ary):
        return ":".join(["%02x" % b for b in ary])
