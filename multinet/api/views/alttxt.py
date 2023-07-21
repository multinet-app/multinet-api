import alttxt
from rest_framework.views import APIView
from rest_framework.response import Response

class AlttxtQueryViewSet(APIView):
    def get(self, request):

        return Response({'message': 'Hello, world!'})