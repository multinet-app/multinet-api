from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import (
    Workspace,
    Table,
)


# The default ModelSerializer for User fails if the user already exists
class UserSerializer(serializers.Serializer):
    username = serializers.CharField(validators=[UnicodeUsernameValidator()])


class UserDetailSerializer(serializers.Serializer):
    username = serializers.CharField(validators=[UnicodeUsernameValidator()])
    first_name = serializers.CharField(validators=[UnicodeUsernameValidator()])
    last_name = serializers.CharField(validators=[UnicodeUsernameValidator()])
    admin = serializers.BooleanField()


# TODO: Add WorkspaceCreateSerializer that this inherits from,
# and specify arnago_db_name on the extended serializer
class WorkspaceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Workspace
        fields = [
            'id',
            'name',
            'created',
            'modified',
        ]
        read_only_fields = ['created']


class TableSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = [
            'id',
            'name',
            'edge',
            'created',
            'modified',
            'workspace',
        ]
        read_only_fields = ['created']

    workspace = WorkspaceSerializer()
