import re

from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import Network, Table, Upload, Workspace
from multinet.api.tasks.process.utils import ColumnTypeEnum


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
class WorkspaceCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Workspace
        fields = [
            'id',
            'name',
            'created',
            'modified',
        ]
        read_only_fields = ['created']


class WorkspaceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Workspace
        fields = WorkspaceCreateSerializer.Meta.fields + [
            'arango_db_name',
        ]
        read_only_fields = ['created']


# ArangoDB Collection/Graph names must abide by this
# TableNetworkCreateNameField = serializers.RegexField(regex=re.compile(r'^[a-zA-Z][a-zA-Z0-9_-]+$'))
TableNetworkCreateNameField = serializers.RegexField(r'^[a-zA-Z][a-zA-Z0-9_-]+$')


# The required fields for table creation
class TableCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = [
            'name',
            'edge',
        ]
        read_only_fields = ['created']
        name = TableNetworkCreateNameField


# Used for full Table serialization / validation
class TableSerializer(TableCreateSerializer):
    class Meta:
        model = Table
        fields = TableCreateSerializer.Meta.fields + [
            'id',
            'created',
            'modified',
            'workspace',
        ]
        read_only_fields = ['created']


# Used for serializing Tables as responses
class TableReturnSerializer(TableSerializer):
    workspace = WorkspaceSerializer()


class NetworkCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = ['name', 'edge_table']

    name = TableNetworkCreateNameField
    edge_table = serializers.CharField()


class NetworkSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = '__all__'


class NetworkReturnSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = ['id', 'name', 'created', 'modified']


class NetworkReturnDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = [
            'id',
            'name',
            'node_count',
            'edge_count',
            'created',
            'modified',
            'workspace',
        ]

    workspace = WorkspaceSerializer()


class UploadCreateSerializer(serializers.Serializer):
    field_value = serializers.CharField()


class UploadReturnSerializer(serializers.ModelSerializer):
    class Meta:
        model = Upload
        fields = '__all__'

    workspace = WorkspaceSerializer()

    # Specify blob as a CharField to coerce to object_key
    blob = serializers.CharField()

    # Specify user as a CharField to return username
    user = serializers.CharField()


class CSVColumnTypeSerializer(serializers.Serializer):
    key = serializers.CharField()
    type = serializers.ChoiceField(choices=ColumnTypeEnum.values())


class CSVUploadCreateSerializer(UploadCreateSerializer):
    edge = serializers.BooleanField()
    table_name = TableNetworkCreateNameField
    columns = serializers.ListField(child=CSVColumnTypeSerializer())
