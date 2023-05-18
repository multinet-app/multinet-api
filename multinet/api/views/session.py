from django.db.models import Q
from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.mixins import (
    CreateModelMixin,
    DestroyModelMixin,
    ListModelMixin,
    RetrieveModelMixin,
)
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from ..models import Network, NetworkSession, Table, TableSession, Workspace
from .serializers import NetworkSessionSerializer, TableSessionSerializer


class SessionCreateSerializer(serializers.Serializer):
    workspace = serializers.CharField()
    network = serializers.CharField(required=False)
    table = serializers.CharField(required=False)

    visapp = serializers.CharField()
    name = serializers.CharField()

    def validate(self, data):
        if not bool(data.get('network')) ^ bool(data.get('table')):
            raise serializers.ValidationError('exactly one of `network` or `table` is required')

        return data


class SessionStatePatchSerializer(serializers.Serializer):
    state = serializers.JSONField()


class SessionViewSet(
    CreateModelMixin, RetrieveModelMixin, DestroyModelMixin, ListModelMixin, GenericViewSet
):
    @swagger_auto_schema(request_body=SessionStatePatchSerializer)
    @action(detail=True, methods=['patch'])
    def state(self, request, pk=None):
        session = self.get_object()

        serializer = SessionStatePatchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data['state']

        session.state = data
        session.save()

        return Response(status=status.HTTP_204_NO_CONTENT)

class NetworkSessionViewSet(SessionViewSet):
    queryset = NetworkSession.objects.all()
    serializer_class = NetworkSessionSerializer


class TableSessionViewSet(SessionViewSet):
    queryset = TableSession.objects.all()
    serializer_class = TableSessionSerializer
