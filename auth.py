import base64, hmac

def check_basic(header, password):
    """HTTP Basic : n'importe quel nom d'utilisateur, mot de passe comparé en temps constant."""
    if not header or not header.lower().startswith("basic "): return False
    try: decoded = base64.b64decode(header[6:]).decode()
    except Exception: return False
    return hmac.compare_digest(decoded.partition(":")[2].encode(), password.encode())
