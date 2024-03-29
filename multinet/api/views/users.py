from __future__ import annotations

from django.contrib.auth.models import User
from django.forms.models import model_to_dict
from django.http.response import HttpResponseBase
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view, parser_classes, permission_classes
from rest_framework.parsers import JSONParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response

from multinet.api.views.serializers import UserDetailSerializer, UserSerializer


@swagger_auto_schema(
    method='GET',
    responses={200: UserDetailSerializer},
)
@api_view(['GET'])
@parser_classes([JSONParser])
@permission_classes([IsAuthenticated])
def users_me_view(request: Request) -> HttpResponseBase:
    """Get the currently authenticated user."""
    user_dict = {'admin': request.user.is_superuser, **model_to_dict(request.user)}

    response_serializer = UserDetailSerializer(user_dict)
    return Response(response_serializer.data)


@swagger_auto_schema(
    method='GET',
    query_serializer=UserSerializer,
    responses={200: UserDetailSerializer(many=True)},
)
@api_view(['GET'])
@parser_classes([JSONParser])
@permission_classes([IsAuthenticated])
def users_search_view(request: Request) -> HttpResponseBase:
    """Search for a user."""
    request_serializer = UserSerializer(data=request.query_params)
    request_serializer.is_valid(raise_exception=True)
    username: str = request_serializer.validated_data['username']

    results = User.objects.filter(username__startswith=username).order_by('username')[:10]

    response_serializer = UserDetailSerializer(results, many=True)
    return Response(response_serializer.data)
