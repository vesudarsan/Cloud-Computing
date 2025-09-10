import requests
from utils.logger import setup_logger

logging = setup_logger(__name__)

class RestClient:
    def __init__(self,  timeout=10):  
        """
        :param base_url: Base URL of the REST service (e.g., 'http://mavlink-service:5002')
        :param timeout: Request timeout in seconds
        """   
        self.timeout = timeout

    def post(self, endpoint, json_data):
        """
        Send POST request to REST service.
        :param endpoint: Endpoint path (e.g., '/mavlink/command')
        :param json_data: JSON payload to send
        :return: Response object or None on failure
        """
      
        url = endpoint

        try:
            logging.info(f"🔗 POST {url}")

            response = requests.post(url, json=json_data, timeout=self.timeout)
           
            if response.status_code == 200:
                logging.info(f"✅ Response: {response.json()}")
            else:
                logging.error(f"❌ Error {response.status_code}: {response.text}")
            return response
        except requests.RequestException as e:
            logging.error(f"REST POST request failed: {e}")
            return None


    def get(self, endpoint, params=None):
        """
        Send GET request to REST service.
        :param endpoint: Endpoint path (e.g., '/mavlink/status')
        :param params: Dictionary of query params (optional)
        :return: Response object or None on failure
        """
        url = endpoint

        try:
            logging.info(f"🔗 GET {url} params={params}")

            response = requests.get(url, params=params, timeout=self.timeout)

            if response.status_code == 200:
                logging.info(f"✅ Response: {response.json()}")
            else:
                logging.error(f"❌ Error {response.status_code}: {response.text}")

            return response

        except requests.RequestException as e:
            logging.error(f"REST GET request failed: {e}")
            return None
