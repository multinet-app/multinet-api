from django.db.models import Q
from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, viewsets
from rest_framework.mixins import CreateModelMixin, RetrieveModelMixin, DestroyModelMixin, ListModelMixin
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from ..models import Network, Session, Table, Workspace
from .serializers import SessionSerializer


class SessionCreateSerializer(serializers.Serializer):
    workspace = serializers.CharField()
    network = serializers.CharField(required=False)
    table = serializers.CharField(required=False)

    name = serializers.CharField()

    def validate(self, data):
        if not bool(data.get('network')) ^ bool(data.get('table')):
            raise serializers.ValidationError('exactly one of `network` or `table` is required')

        return data


class SessionListSerializer(serializers.Serializer):
    workspace = serializers.CharField(required=False)
    network = serializers.CharField(required=False)
    table = serializers.CharField(required=False)

    def validate(self, data):
        workspace = data.get('workspace')
        network = data.get('network')
        table = data.get('table')
        exactly_one = bool(network) ^ bool(table)

        if workspace and not exactly_one:
            raise serializers.ValidationError('if `workspace` is specified, then exactly one of `network` or `table` is required')

        if not workspace and (network or table):
            raise serializers.ValidationError('specifying `network` or `table` requires also specifying `workspace`')

        return data


class SessionViewSet(CreateModelMixin, RetrieveModelMixin, DestroyModelMixin, ListModelMixin, GenericViewSet):
    queryset = Session.objects.all()
    serializer_class = SessionSerializer


    @swagger_auto_schema(
        request_body=SessionCreateSerializer
    )
    def create(self, request):
        serializer = SessionCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        workspace: Workspace = get_object_or_404(Workspace, name=data['workspace'])

        object: Network | Table
        if 'network' in data:
            object = get_object_or_404(Network, workspace=workspace, name=data['network'])
        else:
            object = get_object_or_404(Table, workspace=workspace, name=data['network'])

        new_session = SessionSerializer(
            data = {
                'name': data['name'],
                'workspace': object.pk,
                'network': object.pk if 'network' in data else None,
                'table': object.pk if 'table' in data else None,
                'state': {},
            }
        )
        new_session.is_valid(raise_exception=True)
        new_data = new_session.validated_data

        session = Session.objects.create(
            name=new_data['name'],
            network=new_data['network'],
            table=new_data['table'],
            state={},
        )

        return Response(SessionSerializer(session).data)

    @swagger_auto_schema(
        query_serializer=SessionListSerializer
    )
    def list(self, request):
        serializer = SessionListSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        parent_q: Q = Q()
        if workspace_name := data.get('workspace'):
            workspace: Workspace = get_object_or_404(Workspace, name=workspace_name)

            if name := data.get('table'):
                table = get_object_or_404(Table, workspace=workspace, name=name)
                parent_q = Q(table=table)
            else:
                name = data.get('network')
                network = get_object_or_404(Network, workspace=workspace, name=name)
                parent_q = Q(network=network)

        results = self.get_queryset().filter(parent_q)

        return Response(SessionSerializer(results, many=True).data)
