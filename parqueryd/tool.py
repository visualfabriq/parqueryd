from __future__ import annotations

import errno
import os
import shutil


def ens_bytes(inp: bytes | str) -> bytes:
    """Ensure input is bytes."""
    if isinstance(inp, bytes):
        return inp
    else:
        return bytes(inp.encode("utf-8"))


def ens_unicode(inp: bytes | str) -> str:
    """Ensure input is unicode string."""
    if isinstance(inp, bytes):
        return inp.decode()
    else:
        return inp


def mkdir_p(path: str) -> None:
    """Create directory and parent directories if needed (like mkdir -p)."""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def rm_file_or_dir(path: str | None) -> None:
    """Remove file or directory recursively."""
    if path is not None and os.path.exists(path):
        if os.path.isdir(path):
            if os.path.islink(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
        else:
            if os.path.islink(path):
                os.unlink(path)
            else:
                os.remove(path)
