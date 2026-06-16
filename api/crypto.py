import os

from cryptography.fernet import Fernet

_MASTER_KEY = os.getenv("BOT_MASTER_ENCRYPTION_KEY", "")
_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        if not _MASTER_KEY:
            raise RuntimeError(
                "BOT_MASTER_ENCRYPTION_KEY env var is not set. "
                "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )
        _fernet = Fernet(_MASTER_KEY.encode() if isinstance(_MASTER_KEY, str) else _MASTER_KEY)
    return _fernet


def encrypt(plaintext: str) -> bytes:
    return _get_fernet().encrypt(plaintext.encode("utf-8"))


def decrypt(ciphertext: bytes) -> str:
    return _get_fernet().decrypt(ciphertext).decode("utf-8")
