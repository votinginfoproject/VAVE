from django.db import models
from .storage import FeedFileStorage

fs = FeedFileStorage()

class Feed(models.Model):
    feedfile = models.FileField(upload_to='feeds', storage=fs)
