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


# class WorkspaceDetailSerializer(WorkspaceSerializer):
#     class Meta(WorkspaceSerializer.Meta):
#         fields = WorkspaceSerializer.Meta.fields

#     most_recent_version = VersionSerializer(read_only=True)


# class VersionDetailSerializer(VersionSerializer):
#     class Meta(VersionSerializer.Meta):
#         fields = VersionSerializer.Meta.fields + ['metadata']

#     metadata = serializers.SlugRelatedField(read_only=True, slug_field='metadata')


# class AssetBlobSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = AssetBlob
#         fields = [
#             'uuid',
#             'path',
#             'sha256',
#             'size',
#         ]


# class AssetMetadataSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = AssetMetadata
#         fields = ['metadata']


# class AssetSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Asset
#         fields = [
#             'uuid',
#             'path',
#             'sha256',
#             'size',
#             'created',
#             'modified',
#             'version',
#         ]
#         read_only_fields = ['created']

#     version = VersionSerializer()


# class AssetDetailSerializer(AssetSerializer):
#     class Meta(AssetSerializer.Meta):
#         fields = AssetSerializer.Meta.fields + ['metadata']

#     metadata = serializers.SlugRelatedField(read_only=True, slug_field='metadata')


# class ValidationSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Validation
#         fields = [
#             'state',
#             'sha256',
#             'created',
#             'modified',
#         ]


# class ValidationErrorSerializer(serializers.ModelSerializer):
#     class Meta(ValidationSerializer.Meta):
#         fields = ValidationSerializer.Meta.fields + ['error']
