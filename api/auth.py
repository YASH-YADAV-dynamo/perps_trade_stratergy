import hashlib
import hmac
import os
import time

import jwt
from eth_account.messages import encode_defunct
from web3 import Web3

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import db

_bearer = HTTPBearer()
_w3 = Web3()

_JWT_SECRET = os.environ.get("BOT_JWT_SECRET", "")
_JWT_ALGO = "HS256"
_JWT_EXPIRY_S = 86400  # 24 hours

SIGN_MESSAGE_PREFIX = "Sign in to Bot\nNonce: "


def _get_jwt_secret() -> str:
    secret = _JWT_SECRET or os.environ.get("BOT_MASTER_ENCRYPTION_KEY", "")
    if not secret:
        raise RuntimeError("Neither BOT_JWT_SECRET nor BOT_MASTER_ENCRYPTION_KEY is set")
    return secret


def verify_signature(address: str, nonce: str, signature: str) -> bool:
    message = encode_defunct(text=SIGN_MESSAGE_PREFIX + nonce)
    try:
        recovered = _w3.eth.account.recover_message(message, signature=signature)
    except Exception:
        return False
    return recovered.lower() == address.lower()


def create_jwt(user_id: str, wallet_address: str) -> str:
    now = int(time.time())
    payload = {
        "sub": user_id,
        "wallet": wallet_address.lower(),
        "iat": now,
        "exp": now + _JWT_EXPIRY_S,
    }
    return jwt.encode(payload, _get_jwt_secret(), algorithm=_JWT_ALGO)


def decode_jwt(token: str) -> dict:
    return jwt.decode(token, _get_jwt_secret(), algorithms=[_JWT_ALGO])


async def get_current_user(
    creds: HTTPAuthorizationCredentials = Security(_bearer),
) -> dict:
    try:
        payload = decode_jwt(creds.credentials)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = db.get_user_by_wallet(payload["wallet"])
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def admin_tokens_match(provided: str, expected: str) -> bool:
    if not provided or not expected:
        return False
    hp = hashlib.sha256(provided.encode("utf-8")).hexdigest()
    he = hashlib.sha256(expected.encode("utf-8")).hexdigest()
    return hmac.compare_digest(hp.encode("ascii"), he.encode("ascii"))


async def require_admin(
    creds: HTTPAuthorizationCredentials = Security(_bearer),
) -> None:
    exp = (os.environ.get("DXD_ADMIN_TOKEN") or "").strip()
    if not exp:
        raise HTTPException(
            status_code=503,
            detail="Admin API disabled: set DXD_ADMIN_TOKEN on the server",
        )
    if not admin_tokens_match(creds.credentials, exp):
        raise HTTPException(status_code=401, detail="Invalid admin token")
