from django.contrib.auth.models import User
from django.contrib.auth.validators import UnicodeUsernameValidator
from rest_framework import serializers

from multinet.api.models import (
    AqlQuery,
    Network,
    NetworkSession,
    Table,
    TableSession,
    TableTypeAnnotation,
    Upload,
    Workspace,
)


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
    bind_vars = serializers.DictField(child=serializers.CharField())


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


# Used for serializing Tables as responses
class TableReturnSerializer(TableSerializer):
    workspace = WorkspaceSerializer()


class NetworkCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Network
        fields = ['name', 'edge_table']

    edge_table = serializers.CharField()


# CSV network serializers
class Link(serializers.Serializer):
    local = serializers.CharField()
    foreign = serializers.CharField()


class BaseTable(serializers.Serializer):
    name = serializers.CharField()
    excluded = serializers.ListField(child=serializers.CharField())


class TableJoin(serializers.Serializer):
    table = BaseTable()
    link = Link()


class FullTable(BaseTable):
    joined = TableJoin(required=False)


class EdgeTable(serializers.Serializer):
    table = FullTable()
    source = Link(required=False)
    target = Link(required=False)


class CSVNetworkCreateSerializer(serializers.Serializer):
    name = serializers.CharField()
    edge = EdgeTable()
    source_table = FullTable()
    target_table = FullTable()


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


class NetworkSessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = NetworkSession
        fields = '__all__'


class TableSessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = TableSession
        fields = '__all__'


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


columns_type = serializers.DictField(
    child=serializers.ChoiceField(choices=TableTypeAnnotation.Type.choices),
)


class CSVUploadCreateSerializer(UploadCreateSerializer):
    edge = serializers.BooleanField()
    table_name = serializers.CharField()
    columns = columns_type
    delimiter = serializers.CharField(trim_whitespace=False)
    quotechar = serializers.CharField()


class JSONTableUploadCreateSerializer(UploadCreateSerializer):
    edge = serializers.BooleanField()
    table_name = serializers.CharField()
    columns = columns_type


class JSONNetworkUploadCreateSerializer(UploadCreateSerializer):
    network_name = serializers.CharField()
    node_columns = serializers.DictField(
        child=serializers.ChoiceField(choices=TableTypeAnnotation.Type.choices)
    )
    edge_columns = serializers.DictField(
        child=serializers.ChoiceField(choices=TableTypeAnnotation.Type.choices)
    )
