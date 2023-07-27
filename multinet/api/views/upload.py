from typing import Dict, Optional

from django.core import signing
from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.auth.decorators import require_workspace_permission
from multinet.api.models import Network, Table, Upload, Workspace, WorkspaceRoleChoice
from multinet.api.tasks.upload import process_csv, process_json_network, process_json_table

from .common import MultinetPagination, WorkspaceChildMixin
from .serializers import (
    CSVUploadCreateSerializer,
    JSONNetworkUploadCreateSerializer,
    JSONTableUploadCreateSerializer,
    UploadReturnSerializer,
)

InvalidFieldValueResponse = Response(
    {'field_value': ['field_value is not a valid signed string.']},
    status=status.HTTP_400_BAD_REQUEST,
)


def field_value_object_key(serializer: serializers.Serializer) -> Optional[str]:
    try:
        field_value = serializer.validated_data['field_value']
        field_value_dict: Dict = signing.loads(field_value)
        object_key = field_value_dict['object_key']
    except signing.BadSignature:
        return None

    return object_key


class UploadViewSet(WorkspaceChildMixin, ReadOnlyModelViewSet):
    queryset = Upload.objects.all().select_related('workspace')

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = UploadReturnSerializer

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['uploads']

    @swagger_auto_schema(
        request_body=CSVUploadCreateSerializer(),
        responses={200: UploadReturnSerializer()},
    )
    @action(detail=False, url_path='csv', methods=['POST'])
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def upload_csv(self, request, parent_lookup_workspace__name: str):
        """Create an upload of a CSV file."""
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = CSVUploadCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        object_key = field_value_object_key(serializer)
        if object_key is None:
            return InvalidFieldValueResponse

        # Check for existing table name before creating upload and dispatching task
        table_name = serializer.validated_data['table_name']
        if Table.objects.filter(workspace=workspace, name=table_name).exists():
            return Response(
                {
                    'table_name': [
                        f'Table {table_name} in workspace {workspace.name} already exists.'
                    ]
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Create upload object
        upload: Upload = Upload.objects.create(
            workspace=workspace,
            user=request.user,
            blob=object_key,
            data_type=Upload.DataType.CSV,
        )

        # Dispatch task
        process_csv.delay(
            task_id=upload.pk,
            table_name=table_name,
            edge=serializer.validated_data['edge'],
            columns=serializer.validated_data['columns'],
            delimiter=serializer.validated_data['delimiter'],
            quotechar=serializer.validated_data['quotechar'],
        )

        return Response(UploadReturnSerializer(upload).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=JSONTableUploadCreateSerializer(),
        responses={200: UploadReturnSerializer()},
    )
    @action(detail=False, url_path='json_table', methods=['POST'])
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def upload_json_table(self, request, parent_lookup_workspace__name: str):
        """Create an upload of a JSON table."""
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = JSONTableUploadCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        object_key = field_value_object_key(serializer)
        if object_key is None:
            return InvalidFieldValueResponse

        # Check for existing table name before creating upload and dispatching task
        table_name = serializer.validated_data['table_name']
        if Table.objects.filter(workspace=workspace, name=table_name).exists():
            return Response(
                {
                    'table_name': [
                        f'Table {table_name} in workspace {workspace.name} already exists.'
                    ]
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Create upload object
        upload: Upload = Upload.objects.create(
            workspace=workspace,
            user=request.user,
            blob=object_key,
            data_type=Upload.DataType.JSON_TABLE,
        )

        # Dispatch task
        process_json_table.delay(
            task_id=upload.pk,
            table_name=table_name,
            edge=serializer.validated_data['edge'],
            columns=serializer.validated_data['columns'],
        )

        return Response(UploadReturnSerializer(upload).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=JSONNetworkUploadCreateSerializer(),
        responses={200: UploadReturnSerializer()},
    )
    @action(detail=False, url_path='json_network', methods=['POST'])
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def upload_json_network(self, request, parent_lookup_workspace__name: str):
        """Create an upload of a JSON network file."""
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = JSONNetworkUploadCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        object_key = field_value_object_key(serializer)
        if object_key is None:
            return InvalidFieldValueResponse

        # Check for existing tables and networks before creating upload and dispatching task
        network_name = serializer.validated_data['network_name']
        node_table_name = f'{network_name}_nodes'
        edge_table_name = f'{network_name}_edges'

        # TODO: Make these separate queries more efficient if possible
        if (
            Network.objects.filter(workspace=workspace, name=network_name).exists()
            or Table.objects.filter(workspace=workspace, name=node_table_name).exists()
            or Table.objects.filter(workspace=workspace, name=edge_table_name).exists()
        ):
            return Response(
                {
                    'network_name': [
                        f'Network {network_name} or ancillary tables'
                        f' ({node_table_name}, {edge_table_name})'
                        f' already exist in workspace {workspace.name}.'
                    ]
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Create upload object
        upload: Upload = Upload.objects.create(
            workspace=workspace,
            user=request.user,
            blob=object_key,
            data_type=Upload.DataType.JSON_NETWORK,
        )

        # Dispatch task
        process_json_network.delay(
            task_id=upload.pk,
            network_name=network_name,
            node_table_name=node_table_name,
            edge_table_name=edge_table_name,
            node_column_types=serializer.validated_data['node_columns'],
            edge_column_types=serializer.validated_data['edge_columns'],
        )

        return Response(UploadReturnSerializer(upload).data, status=status.HTTP_200_OK)
