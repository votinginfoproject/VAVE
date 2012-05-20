import re
from django.http import HttpResponse
from oauth2app.authenticate import JSONAuthenticator, AuthenticationException
from oauth2app.models import AccessRange
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from os import path

# For storage
# TODO: externalize this, possibly customize
from django.core.files.uploadhandler import StopFutureHandlers
from django.core.files import File
from .models import Feed
from .handlers import FeedUploadHandler, IncorrectMimeTypeError, FileNameUnspecified

@csrf_exempt
def upload(request):
    request.upload_handlers = [FeedUploadHandler(request=request)]
    return _upload(request)

@csrf_exempt
def _upload(request):
    try:
        if request.method == 'PUT':
            upload_handlers = request.upload_handlers
            content_type = str(request.META.get('CONTENT_TYPE', ""))
            content_length = int(request.META.get('CONTENT_LENGTH', 0))
            charset = ''
            file_name = ''
            field_name = ''

            counters = [0]*len(upload_handlers)

            for handler in upload_handlers:
                try:
                    handler.new_file(
                        field_name,
                        file_name, 
                        content_type,
                        content_length,
                        charset
                    )

                except StopFutureHandlers:
                    break

            #print "Before reading file... Name: {0} Size: {1}".format(file_name, content_length)

            for i, handler in enumerate(upload_handlers):
                chunk = request.read(handler.chunk_size)
                #print "Reading chunk of handler {0}: {1}".format(i, len(chunk))
                while chunk:
                    #print "Tapping the handler"
                    handler.receive_data_chunk(chunk, counters[i])
                    #print "Reading chunk of handler {0}: {1}".format(i, len(chunk))
                    counters[i] += len(chunk)
                    chunk = request.read(handler.chunk_size)

            for i, handler in enumerate(upload_handlers):
                file_obj = handler.file_complete(counters[i])
                if not file_obj:
                    return HttpResponse(content="Death", status=500)
                else:
                    feed = Feed()
                    feed.feedfile = File(file_obj)
                    feed.save()

            return HttpResponse(content="Success", status=200)
            
    except IncorrectMimeTypeError:
        return HttpResponse(content="Incorrect mime type", status=400)

    except FileNameUnspecified:
        return HttpResponse(content="File name not specified", status=400)

    except Exception as e:
        return HttpResponse(content="Death", status=500)
