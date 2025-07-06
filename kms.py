from fastapi import FastAPI, HTTPException, Request
from typing import Set
from cryptography.hazmat.primitives.asymmetric import rsa, ed25519
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64
import os
import uvicorn

app = FastAPI()

ALLOWED_IPS: Set[str] = {"127.0.0.1", "::1"}
KEY_DIR="keys"

# Directory and file paths for storing keys\ nKEY_DIR = "keys"
os.makedirs(KEY_DIR, exist_ok=True)

RSA_PRIVATE_FILE = os.path.join(KEY_DIR, "rsa_private.pem")
RSA_PUBLIC_FILE = os.path.join(KEY_DIR, "rsa_public.pem")
ED_PRIVATE_FILE = os.path.join(KEY_DIR, "ed25519_private.key")
ED_PUBLIC_FILE = os.path.join(KEY_DIR, "ed25519_public.key")

def load_or_generate_rsa():
    if os.path.exists(RSA_PRIVATE_FILE) and os.path.exists(RSA_PUBLIC_FILE):
        # Load RSA keys from files
        with open(RSA_PRIVATE_FILE, 'rb') as f:
            private = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        with open(RSA_PUBLIC_FILE, 'rb') as f:
            public = serialization.load_pem_public_key(
                f.read(), backend=default_backend()
            )
    else:
        # Generate new RSA keypair and save to files
        private = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )
        public = private.public_key()

        # Serialize and write keys
        with open(RSA_PRIVATE_FILE, 'wb') as f:
            f.write(
                private.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
        with open(RSA_PUBLIC_FILE, 'wb') as f:
            f.write(
                public.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            )
    return private, public


def load_or_generate_ed25519():
    if os.path.exists(ED_PRIVATE_FILE) and os.path.exists(ED_PUBLIC_FILE):
        # Load Ed25519 keys from files
        with open(ED_PRIVATE_FILE, 'rb') as f:
            raw_priv = base64.b64decode(f.read())
            ed_private = ed25519.Ed25519PrivateKey.from_private_bytes(raw_priv)
        with open(ED_PUBLIC_FILE, 'rb') as f:
            raw_pub = base64.b64decode(f.read())
            ed_public = ed25519.Ed25519PublicKey.from_public_bytes(raw_pub)
    else:
        # Generate new Ed25519 keypair and save to files
        ed_private = ed25519.Ed25519PrivateKey.generate()
        ed_public = ed_private.public_key()

        # Serialize and write keys
        raw_priv = ed_private.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        raw_pub = ed_public.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        with open(ED_PRIVATE_FILE, 'wb') as f:
            f.write(base64.b64encode(raw_priv))
        with open(ED_PUBLIC_FILE, 'wb') as f:
            f.write(base64.b64encode(raw_pub))

    return ed_private, ed_public

# Load or generate keys on startup
privateKey, publicKey = load_or_generate_rsa()
edPrivateKey, edPublicKey = load_or_generate_ed25519()


def serialize_private_rsa(key: rsa.RSAPrivateKey) -> str:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def serialize_public_rsa(key: rsa.RSAPublicKey) -> str:
    return key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()


def serialize_private_ed(key: ed25519.Ed25519PrivateKey) -> str:
    raw = key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.b64encode(raw).decode()


def serialize_public_ed(key: ed25519.Ed25519PublicKey) -> str:
    raw = key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(raw).decode()

@app.get("/private-key")
async def get_private_key(request: Request):
    if request.client.host not in ALLOWED_IPS:
        raise HTTPException(status_code=403, detail="Forbidden")
    return {"privateKey": serialize_private_rsa(privateKey)}

@app.get("/public-key")
async def get_public_key():
    return {"publicKey": serialize_public_rsa(publicKey)}

@app.get("/ed25519-private-key")
async def get_ed_private_key(request: Request):
    if request.client.host not in ALLOWED_IPS:
        raise HTTPException(status_code=403, detail="Forbidden")
    return {"privateKey": serialize_private_ed(edPrivateKey)}

@app.get("/ed25519-public-key")
async def get_ed_public_key():
    return {"publicKey": serialize_public_ed(edPublicKey)}

if __name__ == "__main__":
    port = int(os.getenv("KMS_PORT", "9000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
