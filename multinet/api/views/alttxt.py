from typing import Type, Any, Union
from pprint import pprint
import json

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.core.files.uploadedfile import UploadedFile

from alttxt.enums import Level, Verbosity, Listable, Explanation, AggregateBy
from alttxt.generator import AltTxtGen
from alttxt.models import DataModel, GrammarModel
from alttxt.parser import Parser
from alttxt.tokenmap import TokenMap

class AlttxtSerializer(serializers.Serializer):
    verbosity = serializers.ChoiceField(choices=[e.value for e in Verbosity])
    level = serializers.ChoiceField(choices=[e.value for e in Level])
    explain = serializers.ChoiceField(choices=[e.value for e in Explanation])
    title = serializers.CharField(max_length=200, default="")
    data = serializers.FileField(required=True)

class AlttxtQueryViewSet(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request) -> Response:
        """
        Process and respond to a form-multipart request
        containing the params and data needed to generate an alttxt
        """

        # Get data and serialize it
        serializer = AlttxtSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        # Fields into variables
        parsed_data = serializer.validated_data
        verbosity = Verbosity(parsed_data['verbosity'])
        level = Level(parsed_data['level'])
        explain = Explanation(parsed_data['explain'])
        title = parsed_data['title']
        datafile = parsed_data['data']

        # Load the data
        try:
            if isinstance(datafile, UploadedFile):
                data: dict = json.loads(datafile.read())
            elif isinstance(datafile, str):
                data: dict = json.loads(datafile)
            else:
                return Response({'error': 'Invalid data file: must be a JSON file or string'}, status=status.HTTP_400_BAD_REQUEST)
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