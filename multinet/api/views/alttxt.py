from typing import Type
from pprint import pprint
from rest_framework.views import APIView
from rest_framework.response import Response
from alttxt.enums import Level, Verbosity, Listable
from rest_framework import status

class AlttxtQueryViewSet(APIView):
    def get(self, request) -> Response:

        def inEnum(enum: Type[Listable], value: str) -> bool:
            """
            Checks if a value is in an enum
            """
            for e in enum:
                if e.value == value:
                    return True
            return False

        verbosity: str = request.GET.get('verbosity', None)
        level: str = request.GET.get('level', None)

        # Validation
        if verbosity is None or not inEnum(Verbosity, verbosity):
            return Response({'error': 'Invalid or missing verbosity parameter'}, status=status.HTTP_400_BAD_REQUEST)

        if level is None or not inEnum(Level, level):
            return Response({'error': 'Invalid or missing level parameter'}, status=status.HTTP_400_BAD_REQUEST)

        # Now the variables can be used in the next part of the program
        verbosity = Verbosity(verbosity)
        level = Level(level)

        return Response({'message': 'Hello, world!'})
    
    # Not sure why, but this method appears necessary to avoid a crash
    def get_extra_actions():
        return []