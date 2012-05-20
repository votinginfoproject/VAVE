"""
Upload handlers for the upload API.
"""
import re
from django.core.files.uploadhandler import TemporaryFileUploadHandler, StopUpload
from django.core.files.uploadedfile import TemporaryUploadedFile

class IncorrectMimeTypeError(StopUpload):
    pass

class FileNameUnspecified(StopUpload):
    pass

class FeedUploadHandler(TemporaryFileUploadHandler):
    """
    This handler specifically handles feed uploads
    """
    QUOTA = 42 * 2**20 # 42 MB
    # doesn't seem to be a good way to identify zip files
    MIME_TYPES = (
        'application/zip',
        'application/x-zip',
        'application/x-gzip',
    )

    def __init__(self, *args, **kwargs):
        super(FeedUploadHandler, self).__init__(*args, **kwargs)
        self.total_upload = 0
        self.file_name = ""

    def _validate_file(self):
        filename_re = re.compile(r'filename="(?P<name>[^"]+)"')
        content_type = str(self.request.META.get('CONTENT_TYPE', ""))
        content_length = int(self.request.META.get('CONTENT_LENGTH', 0))
        charset = 'binary'

        m = filename_re.search(self.request.META.get("HTTP_CONTENT_DISPOSITION", ""))

        if content_type not in self.MIME_TYPES:
            raise IncorrectMimeTypeError("Incorrect mime type", connection_reset=True)
        if content_length > self.QUOTA:
            raise StopUpload(connection_reset=True)
        if not m:
            raise FileNameUnspecified("File name not specified", connection_reset=True)

        self.file_name = self.file_name = m.group('name')
        self.content_type = content_type
        self.content_length = content_length
#        print content_length

    def new_file(self, file_name, *args, **kwargs):
        """
        Create the file object to append to as data is coming in.
        Ignores and overwrites most of the arguments and relies on exsiting request
        """
        super(FeedUploadHandler, self).new_file(file_name, *args, **kwargs)
        self._validate_file()
        self.file = TemporaryUploadedFile(self.file_name, self.content_type, 0, self.charset)

    def receive_data_chunk(self, raw_data, start):
        self.total_upload += len(raw_data)

#        print "Total upload: {0}".format(self.total_upload)

        if self.total_upload >= self.QUOTA:
            raise StopUpload(connection_reset=True)

        self.file.write(raw_data)
