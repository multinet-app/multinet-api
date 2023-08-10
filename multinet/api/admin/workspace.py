from django.contrib import admin
from guardian.admin import GuardedModelAdmin

from multinet.api.models import Workspace


@admin.register(Workspace)
class WorkspaceAdmin(GuardedModelAdmin):
    list_display = ['id', 'name', 'arango_db_name', 'created', 'modified', 'public', 'starred']
    readonly_fields = ['id', 'created']
