from django.shortcuts import get_object_or_404
from rest_framework import viewsets
from rest_framework.response import Response

from ..models import Network, Session, Workspace
from .serializers import SessionSerializer


class SessionViewSet(viewsets.ModelViewSet):
    queryset = Session.objects.all()
    serializer_class = SessionSerializer

    def create(self, request, parent_lookup_workspace__name, parent_lookup_network_pk):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, id=parent_lookup_network_pk)

        serializer = SessionSerializer(
            data = {
                'name': request.data.get('name'),
                'workspace': workspace.pk,
                'network': network.pk,
                'state': {},
            }
        )
        serializer.is_valid(raise_exception=True)

        session = Session.objects.create(
            name=serializer.validated_data['name'],
            network=network,
            state=serializer.validated_data['state'],
        )

        return Response(SessionSerializer(session).data)

    def list(self, request, parent_lookup_workspace__name, parent_lookup_network_pk):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, id=parent_lookup_network_pk)

        return Response(SessionSerializer(self.get_queryset(), many=True).data)
