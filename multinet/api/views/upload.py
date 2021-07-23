from typing import Dict, Optional

from django.core import signing
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from drf_yasg.utils import swagger_auto_schema
from guardian.decorators import permission_required_or_403
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Network, Table, Upload, Workspace
from multinet.api.tasks.process import process_csv, process_d3_json

from .common import MultinetPagination
from .serializers import (
    CSVUploadCreateSerializer,
    D3JSONUploadCreateSerializer,
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


class UploadViewSet(NestedViewSetMixin, ReadOnlyModelViewSet):
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
    @method_decorator(
        permission_required_or_403('owner', (Workspace, 'name', 'parent_lookup_workspace__name'))
    )
    @action(detail=False, url_path='csv', methods=['POST'])
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
            upload.pk,
            table_name=table_name,
            edge=serializer.validated_data['edge'],
            columns=serializer.validated_data['columns'],
        )

        return Response(UploadReturnSerializer(upload).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=D3JSONUploadCreateSerializer(),
        responses={200: UploadReturnSerializer()},
    )
    @method_decorator(
        permission_required_or_403('owner', (Workspace, 'name', 'parent_lookup_workspace__name'))
    )
    @action(detail=False, url_path='d3_json', methods=['POST'])
    def upload_d3_json(self, request, parent_lookup_workspace__name: str):
        """Create an upload of a D3 JSON file."""
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = D3JSONUploadCreateSerializer(data=request.data)
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
            data_type=Upload.DataType.D3_JSON,
        )

        # Dispatch task
        process_d3_json.delay(
            upload.pk,
            network_name=network_name,
            node_table_name=node_table_name,
            edge_table_name=edge_table_name,
        )

        return Response(UploadReturnSerializer(upload).data, status=status.HTTP_200_OK)