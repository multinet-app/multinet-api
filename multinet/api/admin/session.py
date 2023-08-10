from django.contrib import admin
from guardian.admin import GuardedModelAdmin

from multinet.api.models import NetworkSession, TableSession


@admin.register(NetworkSession)
class NetworkSessionAdmin(GuardedModelAdmin):
    list_display = ['id', 'name', 'created', 'modified', 'starred', 'network']
    readonly_fields = ['id', 'created']


@admin.register(TableSession)
class TableSessionAdmin(GuardedModelAdmin):
    list_display = ['id', 'name', 'created', 'modified', 'starred']
    readonly_fields = ['id', 'created']
