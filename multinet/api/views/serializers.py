from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import AqlQuery, Network, Table, TableTypeAnnotation, Upload, Workspace


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


class WorkspaceRenameSerializer(serializers.ModelSerializer):
    class Meta:
        model = Workspace
        fields = [
            'name',
        ]


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


class SingleUserWorkspacePermissionSerializer(serializers.Serializer):
    # Allow empty username since anonymous user is a reader for public workspaces
    username = serializers.CharField(validators=[UnicodeUsernameValidator()], allow_blank=True)
    workspace = serializers.CharField()
    permission = serializers.IntegerField(allow_null=True)
    permission_label = serializers.CharField(allow_null=True)


class AqlQuerySerializer(serializers.Serializer):
    query = serializers.CharField()


class AqlQueryTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = AqlQuery
        exclude = ['results']

    workspace = WorkspaceSerializer()

    # Specify user as a CharField to return username
    user = serializers.CharField()


class AqlQueryResultsSerializer(serializers.ModelSerializer):
    class Meta:
        model = AqlQuery
        fields = ['id', 'workspace', 'user', 'results']

    workspace = serializers.CharField()
    user = serializers.CharField()


class LimitOffsetSerializer(serializers.Serializer):
    limit = serializers.IntegerField(required=False)
    offset = serializers.IntegerField(required=False)


class PaginatedResultSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    previous = serializers.URLField(allow_null=True)
    next = serializers.URLField(allow_null=True)
    results = serializers.ListField(child=serializers.JSONField())


class TableRowRetrieveSerializer(LimitOffsetSerializer):
    filter = serializers.JSONField(required=False)


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
        ]
        read_only_fields = ['created']


class TableMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = TableTypeAnnotation
        fields = ['table', 'column']

    type = serializers.ChoiceField(choices=TableTypeAnnotation.Type.choices)


# Used for serializing Tables as responses
class TableReturnSerializer(TableSerializer):
    workspace = WorkspaceSerializer()


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


class NetworkTablesSerializer(serializers.Serializer):
    type = serializers.ChoiceField(choices=['node', 'edge', 'all'], default='all', required=False)


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
        child=serializers.ChoiceField(choices=TableTypeAnnotation.Type.choices),
        default=dict,
    )


class D3JSONUploadCreateSerializer(UploadCreateSerializer):
    network_name = serializers.CharField()
