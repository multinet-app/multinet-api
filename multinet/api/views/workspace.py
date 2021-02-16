# from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.shortcuts import assign_perm
from guardian.utils import get_40x_or_None
from rest_framework import status

from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Workspace
from multinet.api.views.serializers import WorkspaceSerializer


class WorkspaceViewSet(ReadOnlyModelViewSet):
    queryset = Workspace.objects.all()
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = WorkspaceSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = PageNumberPagination

    @swagger_auto_schema(
        request_body=WorkspaceSerializer(),
        responses={200: WorkspaceSerializer()},
    )
    def create(self, request):
        serializer = WorkspaceSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        workspace, created = Workspace.objects.get_or_create(
            name=serializer.validated_data['name'],
        )

        if created:
            workspace.save()

        assign_perm('owner', request.user, workspace)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    # @permission_required_or_403('owner', (Workspace, 'dandiset__pk'))
    def destroy(self, request, pk):
        workspace: Workspace = get_object_or_404(Workspace, pk=pk)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

        workspace.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)
