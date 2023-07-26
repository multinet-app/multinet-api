import alttxt
from rest_framework.views import APIView
from rest_framework.response import Response

class AlttxtQueryViewSet(APIView):
    def get(self, request):

        return Response({'message': 'Hello, world!'})
    
    # Not sure why, but this method appears necessary to avoid a crash
    def get_extra_actions():
        return []