import jwt
import hashlib
import uuid
from urllib.parse import urlencode
from typing import Any, Dict, Optional

"""
Upbit uses JWT for authentication
More info on authentication: https://beta-docs.upbit.com/docs/create-authorization-request
"""


class UpbitAuth:

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def generate_token(self, query: Optional[Dict[str, Any]] = None) -> (Dict[str, Any]):
        if query:
            query_string = urlencode(query).encode()
            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            auth_payload = {
                'access_key': self.api_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }
        else:
            auth_payload = {
                'access_key': self.api_key,
                'nonce': str(uuid.uuid4()),
            }

        token = jwt.encode(auth_payload, self.secret_key, 'HS256')

        return token

    def get_headers(self, query: Optional[Dict[str, Any]] = None) -> (Dict[str, Any]):
        token = self.generate_token(query)

        return {
            "Authorization": f"Bearer {token.decode('utf-8')}"
        }
