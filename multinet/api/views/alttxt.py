from typing import Union
import json

from rest_framework.viewsets import ReadOnlyModelViewSet
from django.http import JsonResponse
from rest_framework.exceptions import ValidationError, ParseError
from rest_framework import status, serializers
from django.core.files.uploadedfile import UploadedFile

from alttxt.enums import Level, Verbosity, Explanation, AggregateBy
from alttxt.generator import AltTxtGen
from alttxt.models import DataModel, GrammarModel
from alttxt.parser import Parser
from alttxt.tokenmap import TokenMap

class AlttxtSerializer(serializers.Serializer):
    verbosity = serializers.ChoiceField(choices=Verbosity.list())
    level = serializers.ChoiceField(choices=Level.list())
    explain = serializers.ChoiceField(choices=Explanation.list())
    title = serializers.CharField(max_length=200, default="")
    data = serializers.FileField(required=True)

class AlttxtQueryViewSet(ReadOnlyModelViewSet):
    authentication_classes = []
    permission_classes = []

    def post(self, request) -> JsonResponse:
        """
        Process and respond to a form-multipart request
        containing the params and data needed to generate an alttxt
        """

        # Get data and serialize it
        serializer = AlttxtSerializer(data=request.data)
        if not serializer.is_valid():
            raise ValidationError(serializer.errors)

        # Fields into variables
        parsed_data = serializer.validated_data
        verbosity: Verbosity = Verbosity(parsed_data['verbosity'])
        level: Level = Level(parsed_data['level'])
        explain: Explanation = Explanation(parsed_data['explain'])
        title: str = parsed_data['title']
        datafile: Union[UploadedFile, str] = parsed_data['data']

        # Load the data
        try:
            if isinstance(datafile, UploadedFile):
                data: dict = json.loads(datafile.read())
            elif isinstance(datafile, str):
                data: dict = json.loads(datafile)
            else:
                raise ParseError('Invalid data file: must be a JSON file or string')
        except json.decoder.JSONDecodeError as e:
            raise ParseError(f'Invalid JSON: error while parsing: {e.msg}')
        
        # Validate the data
        try:
            if not isinstance(data, dict) or "firstAggregateBy" not in data or \
                AggregateBy(data["firstAggregateBy"]) != AggregateBy.NONE:

                raise ValidationError('Invalid data file: JSON must not be aggregated')
        except ValueError:
            raise ValidationError(f'Invalid data file: {data["firstAggregateBy"]} is not a valid aggregation type')

        # Now parse & generate the alttxt
        parser: Parser = Parser(data)
        try:
            grammar: GrammarModel = parser.get_grammar()
            data: DataModel = parser.get_data()
        except ValueError as e:
            raise ValidationError("Error while parsing data: " + str(e))
        
        tokenmap: TokenMap = TokenMap(data, grammar, title)
        generator: AltTxtGen = AltTxtGen(level, verbosity, explain, tokenmap, grammar)

        return JsonResponse({'alttxt': generator.text})
