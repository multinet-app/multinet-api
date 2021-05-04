from django.contrib import admin
from guardian.admin import GuardedModelAdmin

from multinet.api.models import Upload


@admin.register(Upload)
class UploadAdmin(GuardedModelAdmin):
    list_display = ['id', 'created', 'modified', 'workspace', 'blob']
    readonly_fields = ['id', 'created']
