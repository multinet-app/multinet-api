from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import (
    Workspace,
)


# The default ModelSerializer for User fails if the user already exists
class UserSerializer(serializers.Serializer):
    username = serializers.CharField(validators=[UnicodeUsernameValidator()])


class UserDetailSerializer(serializers.Serializer):
    username = serializers.CharField(validators=[UnicodeUsernameValidator()])
    first_name = serializers.CharField(validators=[UnicodeUsernameValidator()])
    last_name = serializers.CharField(validators=[UnicodeUsernameValidator()])
    admin = serializers.BooleanField()


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
