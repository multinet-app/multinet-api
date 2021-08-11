from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import Network, Table, Upload, Workspace
from multinet.api.tasks.process.utils import ColumnTypeEnum


# The default ModelSerializer for User fails if the user already exists
class UserSerializer(serializers.Serializer):
    username = serializers.CharField(validators=[UnicodeUsernameValidator()])


class UserDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = [
            'id',
            'username',
            'email',
            'first_name',
            'last_name',
            'is_superuser',
        ]


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
            'public',
        ]
        read_only_fields = ['created']


class WorkspaceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Workspace
        fields = WorkspaceCreateSerializer.Meta.fields + [
            'arango_db_name',
        ]
        read_only_fields = ['created']


class PermissionsCreateSerializer(serializers.Serializer):
    public = serializers.BooleanField()
    owner = UserSerializer()
    maintainers = UserSerializer(many=True)
    writers = UserSerializer(many=True)
    readers = UserSerializer(many=True)


class PermissionsReturnSerializer(serializers.ModelSerializer):
    owner = UserDetailSerializer()
    maintainers = UserDetailSerializer(many=True)
    writers = UserDetailSerializer(many=True)
    readers = UserDetailSerializer(many=True)

    class Meta:
        model = Workspace
        fields = WorkspaceCreateSerializer.Meta.fields + [
            'owner',
            'maintainers',
            'writers',
            'readers',
        ]


# The required fields for table creation
class TableCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = [
            'name',
            'edge',
        ]
        read_only_fields = ['created']


# Used for full Table serialization / validation
class TableSerializer(TableCreateSerializer):
    class Meta:
        model = Table
        fields = TableCreateSerializer.Meta.fields + [
            'id',
            'created',
            'modified',
            'workspace',
            'metadata',
        ]
        read_only_fields = ['created']


# Used for serializing Tables as responses
class TableReturnSerializer(TableSerializer):
    def __init__(self, *args, **kwargs):

        if 'show_metadata' not in kwargs.keys():
            super().__init__(*args, **kwargs)
            self.fields.pop('metadata')
        else:
            show_metadata = kwargs.pop('show_metadata')
            super().__init__(*args, **kwargs)
            if not show_metadata:
                self.fields.pop('show_metadata')

    workspace = WorkspaceSerializer()


class ColumnTypeField(serializers.Field):
    """Column types are serialized to their value"""

    def to_representation(self, value):
        return value.value

    def to_internal_value(self, data):
        return ColumnTypeEnum(data)


class ColumnTypeSerializer(serializers.Serializer):
    key: serializers.CharField()
    type: ColumnTypeField()


class NetworkCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = ['name', 'edge_table']

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


class CSVUploadCreateSerializer(UploadCreateSerializer):
    edge = serializers.BooleanField()
    table_name = serializers.CharField()
    columns = serializers.DictField(
        child=serializers.ChoiceField(choices=ColumnTypeEnum.values()),
        default=dict,
    )


class D3JSONUploadCreateSerializer(UploadCreateSerializer):
    network_name = serializers.CharField()
