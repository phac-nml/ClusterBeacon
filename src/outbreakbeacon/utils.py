import hashlib

def calc_md5(list):
    """
    Method to encode the MD5 hash for the input string.

    Parameters
    ----------
    string : srt
    The string to compute the MD5 hash

    Returns
    -------
    hash
        The md5 hash generated
    """
    out = []
    for string in list:
        seq = str(string).encode()
        md5 = hashlib.md5()
        md5.update(seq)
        out.append(md5.hexdigest())
    return out