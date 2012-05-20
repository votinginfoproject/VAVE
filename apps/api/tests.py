from django.utils import unittest
from django.test.client import Client

class UploadFeedTest(unittest.TestCase):
    def setUp(self):
        # startup the client
        self.client = Client()

    def test_details(self):
        with open('/path/to/feed/file/vipFeed-00-2012-01-01.zip', 'rb') as feed:
            request = {
                'content_type': 'application/zip',
                'data': {
                    'filename': 'vipFeed-00-2012-01-01.zip',
                    'attachment': feed,
                },
                'extra': {
                    'HTTP_CONTENT_LENGTH': 3715219,
                    'HTTP_IF_MATCH': 'vipFeed-00-2012-01-01.zip'
                }
            }

            # issue a PUT request
            response = self.client.put("/api/feed/upload/", **request)
            
            # Check that the response is 200 OK
            self.assertEqual(response.status_code, 200)
