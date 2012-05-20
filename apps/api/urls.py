from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('apps.api.views',
    (r'^feed/upload/?$', 'upload')
)
