from typing import Type, Any
from pprint import pprint
import json

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view, authentication_classes, permission_classes

from alttxt.enums import Level, Verbosity, Listable, Explanation, AggregateBy
from alttxt.generator import AltTxtGen
from alttxt.models import DataModel, GrammarModel
from alttxt.parser import Parser
from alttxt.tokenmap import TokenMap

class AlttxtQueryViewSet(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request) -> Response:

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
        explain: str = request.GET.get('explain', None)
        title: str = request.GET.get('title', "")
        datafile = request.FILES.get('data', None)

        # Validation
        if verbosity is None or not inEnum(Verbosity, verbosity):
            return Response({'error': 'Invalid or missing verbosity parameter'}, status=status.HTTP_400_BAD_REQUEST)
        if level is None or not inEnum(Level, level):
            return Response({'error': 'Invalid or missing level parameter'}, status=status.HTTP_400_BAD_REQUEST)
        if explain is None or not inEnum(Explanation, explain):
            return Response({'error': 'Invalid or missing explain parameter'}, status=status.HTTP_400_BAD_REQUEST)
        if datafile is None:
            return Response({'error': 'Missing data + state JSON export'}, status=status.HTTP_400_BAD_REQUEST)

        # Now the variables can be used in the next part of the program
        verbosity = Verbosity(verbosity)
        level = Level(level)
        explain = Explanation(explain)

        # Load the data
        try:
            data: dict[str, Any] = json.loads(datafile.read())
        except json.decoder.JSONDecodeError as e:
            return Response({'error': f'Invalid JSON: error while parsing: {e.msg}'}, status=status.HTTP_400_BAD_REQUEST)
        # Validate the data
        if not isinstance(data, dict) or "firstAggregateBy" not in data or \
            AggregateBy(data["firstAggregateBy"]) != AggregateBy.NONE:

            return Response({'error': 'Invalid data file: JSON must not be aggregated'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Now parse & generate the alttxt
        parser: Parser = Parser(data)
        grammar: GrammarModel = parser.get_grammar()
        data: DataModel = parser.get_data()
        tokenmap: TokenMap = TokenMap(data, grammar, title)
        generator: AltTxtGen = AltTxtGen(level, verbosity, explain, tokenmap, grammar)

        return Response({'alttxt': generator.text})
    
    # Not sure why, but this method appears necessary to avoid a crash
    def get_extra_actions():
        return []