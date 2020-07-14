import hmac
import time
import hashlib
from novadax import RequestClient as NovaClient

class NovadaxAuth:
    def __init__(self, novadax_client: NovaClient):
        http = nova._http
        auth = http._auth
        self.api_key = auth._access_key
        self.secret_key = auth._secret_key

    def api_key(self):
        return self.api_key

    def secret_key(self):
        return self.secret_key

